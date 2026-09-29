//! Loads `reference/test/fixtures/*.yml` the way `ActiveRecord::FixtureSet` does, so Rust
//! tests run against the same rows as the Ruby tests:
//!
//! - ids are `Zlib.crc32(label) % (2**30 - 1)` unless given;
//! - `belongs_to` values are labels (`creator: :david`), polymorphic ones name the class
//!   (`record: first (Message)`);
//! - enum names become their stored values (`role: administrator` is 1);
//! - `created_at`/`updated_at` default to one `now` per fixture file;
//! - columns a fixture leaves out get the column default;
//! - each table is emptied, then filled, with foreign keys checked at commit.
//!
//! ERB is evaluated for the forms the fixtures use: `<%= N.<unit>.ago %>`,
//! `BCrypt::Password.create("...")` assigned to a local, and `User.generate_bot_token`.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use jiff::SignedDuration;
use rusqlite::Connection;
use rusqlite::types::Value;
use serde_yaml_ng::Value as Yaml;

use crate::error::{Error, Result};
use crate::models::{Involvement, Role, Status, user};
use crate::time::Timestamp;

/// `ActiveRecord::FixtureSet::MAX_ID`
pub const MAX_ID: u32 = (1 << 30) - 1;

/// `ActiveRecord::FixtureSet.identify(label)`
pub fn identify(label: &str) -> i64 {
    i64::from(crc32fast::hash(label.as_bytes()) % MAX_ID)
}

/// The fixture directory in the reference checkout.
pub fn reference_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../reference/test/fixtures")
}

#[derive(Debug, Clone)]
pub struct Options {
    /// Base time for `N.minutes.ago` and the default timestamps.
    pub now: Timestamp,
    /// BCrypt cost for `BCrypt::Password.create` (bcrypt-ruby's default is 12).
    pub bcrypt_cost: u32,
}

/// What was loaded: table -> label -> id.
#[derive(Debug, Clone, Default)]
pub struct Loaded {
    pub ids: BTreeMap<String, BTreeMap<String, i64>>,
}

impl Loaded {
    pub fn id(&self, table: &str, label: &str) -> Option<i64> {
        self.ids.get(table)?.get(label).copied()
    }
}

/// A `belongs_to` in a fixture: the key, and the column it sets (or, for polymorphic
/// ones, the `_id`/`_type` pair).
enum Association {
    BelongsTo {
        key: &'static str,
        column: &'static str,
    },
    Polymorphic {
        key: &'static str,
    },
}

fn associations(table: &str) -> &'static [Association] {
    use Association::*;
    match table {
        "boosts" => &[
            BelongsTo {
                key: "message",
                column: "message_id",
            },
            BelongsTo {
                key: "booster",
                column: "booster_id",
            },
        ],
        "memberships" => &[
            BelongsTo {
                key: "room",
                column: "room_id",
            },
            BelongsTo {
                key: "user",
                column: "user_id",
            },
        ],
        "messages" => &[
            BelongsTo {
                key: "room",
                column: "room_id",
            },
            BelongsTo {
                key: "creator",
                column: "creator_id",
            },
        ],
        "rooms" => &[BelongsTo {
            key: "creator",
            column: "creator_id",
        }],
        "bans" | "searches" | "sessions" | "webhooks" | "push_subscriptions" => &[BelongsTo {
            key: "user",
            column: "user_id",
        }],
        "action_text_rich_texts" => &[Polymorphic { key: "record" }],
        "active_storage_attachments" => &[
            Polymorphic { key: "record" },
            BelongsTo {
                key: "blob",
                column: "blob_id",
            },
        ],
        "active_storage_variant_records" => &[BelongsTo {
            key: "blob",
            column: "blob_id",
        }],
        _ => &[],
    }
}

/// Enum attributes: name -> stored value.
fn resolve_enum(table: &str, column: &str, value: &str) -> Option<Value> {
    match (table, column) {
        ("users", "role") => Role::from_name(value).map(|r| Value::Integer(r as i64)),
        ("users", "status") => Status::from_name(value).map(|s| Value::Integer(s as i64)),
        ("memberships", "involvement") => {
            Involvement::from_name(value).map(|i| Value::Text(i.name().into()))
        }
        _ => None,
    }
}

/// Loads every `*.yml` under `dir` into `conn`, inside the caller's transaction or its own.
pub fn load(conn: &Connection, dir: &Path, options: &Options) -> Result<Loaded> {
    let mut files = Vec::new();
    collect_yaml_files(dir, dir, &mut files)?;
    files.sort();

    let autocommit = conn.is_autocommit();
    if autocommit {
        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
    }
    let result = load_files(conn, dir, &files, options);
    if autocommit {
        match &result {
            Ok(_) => conn.execute_batch("COMMIT TRANSACTION")?,
            Err(_) => conn.execute_batch("ROLLBACK TRANSACTION")?,
        }
    }
    result
}

fn load_files(
    conn: &Connection,
    dir: &Path,
    files: &[PathBuf],
    options: &Options,
) -> Result<Loaded> {
    conn.execute_batch("PRAGMA defer_foreign_keys = ON")?;
    let mut erb = Erb::new(options);
    let mut loaded = Loaded::default();
    for file in files {
        let table = table_name(dir, file);
        let source = std::fs::read_to_string(file)
            .map_err(|e| Error::Other(format!("{}: {e}", file.display())))?;
        let yaml = erb.render(&source)?;
        let rows: Yaml = serde_yaml_ng::from_str(&yaml)
            .map_err(|e| Error::Other(format!("{}: {e}", file.display())))?;

        conn.execute(&format!(r#"DELETE FROM "{table}""#), [])?;
        let columns = table_columns(conn, &table)?;
        let now = Timestamp::from_jiff(options.now.jiff());
        let labels = loaded.ids.entry(table.clone()).or_default();

        let Yaml::Mapping(rows) = rows else { continue };
        for (label, row) in rows {
            let label = scalar_string(&label)
                .ok_or_else(|| Error::Other(format!("bad label in {table}")))?;
            if label == "DEFAULTS" || label == "_fixture" {
                continue;
            }
            let row = fixture_row(&table, &label, row, &columns, now)?;
            let id = match row.get("id") {
                Some(Value::Integer(id)) => *id,
                _ => identify(&label),
            };
            labels.insert(label, id);
            insert_row(conn, &table, &row)?;
        }
    }
    Ok(loaded)
}

fn fixture_row(
    table: &str,
    label: &str,
    row: Yaml,
    columns: &[String],
    now: Timestamp,
) -> Result<BTreeMap<String, Value>> {
    let mut values: BTreeMap<String, Value> = BTreeMap::new();
    let mapping = match row {
        Yaml::Mapping(m) => m,
        Yaml::Null => Default::default(),
        _ => {
            return Err(Error::Other(format!(
                "fixture {table}.{label} is not a mapping"
            )));
        }
    };

    for (key, value) in mapping {
        let key = scalar_string(&key)
            .ok_or_else(|| Error::Other(format!("bad key in {table}.{label}")))?;
        let association = associations(table).iter().find(|a| match a {
            Association::BelongsTo { key: k, .. } | Association::Polymorphic { key: k } => {
                *k == key
            }
        });
        match association {
            Some(Association::BelongsTo { column, .. }) => {
                let target = scalar_string(&value)
                    .ok_or_else(|| Error::Other(format!("{table}.{label}.{key}")))?;
                values.insert(
                    (*column).into(),
                    Value::Integer(identify(target.trim_start_matches(':'))),
                );
            }
            Some(Association::Polymorphic { key }) => {
                let target = scalar_string(&value)
                    .ok_or_else(|| Error::Other(format!("{table}.{label}.{key}")))?;
                let (target_label, class) = match target
                    .trim()
                    .strip_suffix(')')
                    .and_then(|t| t.rsplit_once(" ("))
                {
                    Some((l, c)) => (l.to_string(), c.to_string()),
                    None => {
                        return Err(Error::Other(format!(
                            "{table}.{label}.{key} needs a (Class)"
                        )));
                    }
                };
                values.insert(
                    format!("{key}_id"),
                    Value::Integer(identify(target_label.trim_start_matches(':'))),
                );
                values.insert(format!("{key}_type"), Value::Text(class));
            }
            None => {
                let value = yaml_to_sql(table, &key, value)?;
                values.insert(key, value);
            }
        }
    }

    values
        .entry("id".into())
        .or_insert_with(|| Value::Integer(identify(label)));
    for column in ["created_at", "updated_at"] {
        if columns.iter().any(|c| c == column) {
            values
                .entry(column.into())
                .or_insert_with(|| Value::Text(now.to_db()));
        }
    }
    Ok(values)
}

fn yaml_to_sql(table: &str, column: &str, value: Yaml) -> Result<Value> {
    Ok(match value {
        Yaml::Null => Value::Null,
        Yaml::Bool(b) => Value::Integer(b as i64),
        Yaml::Number(n) => match n.as_i64() {
            Some(i) => Value::Integer(i),
            None => Value::Real(n.as_f64().unwrap_or_default()),
        },
        Yaml::String(s) => {
            if let Some(v) = resolve_enum(table, column, &s) {
                v
            } else if is_datetime_column(column) {
                // Time columns cast the text (e.g. ERB's "2026-09-26 11:24:38 UTC").
                match Timestamp::parse_db(&s) {
                    Some(ts) => Value::Text(ts.to_db()),
                    None => Value::Text(s),
                }
            } else {
                Value::Text(s)
            }
        }
        other => Value::Text(
            serde_yaml_ng::to_string(&other)
                .unwrap_or_default()
                .trim()
                .to_string(),
        ),
    })
}

fn is_datetime_column(column: &str) -> bool {
    column.ends_with("_at")
}

fn insert_row(conn: &Connection, table: &str, row: &BTreeMap<String, Value>) -> Result<()> {
    let columns: Vec<String> = row.keys().map(|c| format!(r#""{c}""#)).collect();
    let sql = format!(
        r#"INSERT INTO "{table}" ({}) VALUES ({})"#,
        columns.join(", "),
        crate::sql::placeholders(row.len())
    );
    conn.execute(&sql, rusqlite::params_from_iter(row.values()))?;
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!(r#"PRAGMA table_info("{table}")"#))?;
    let columns = stmt
        .query_map([], |r| r.get::<_, String>("name"))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Err(Error::Other(format!("no table {table}")));
    }
    Ok(columns)
}

/// `action_text/rich_texts.yml` -> `action_text_rich_texts`, `push/subscriptions.yml` ->
/// `push_subscriptions` (the model's table name).
fn table_name(dir: &Path, file: &Path) -> String {
    let relative = file.strip_prefix(dir).unwrap_or(file).with_extension("");
    relative.to_string_lossy().replace(['/', '\\'], "_")
}

fn collect_yaml_files(root: &Path, dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        std::fs::read_dir(dir).map_err(|e| Error::Other(format!("{}: {e}", dir.display())))?
    {
        let path = entry.map_err(|e| Error::Other(e.to_string()))?.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n != "files") {
                collect_yaml_files(root, &path, files)?;
            }
        } else if path.extension().is_some_and(|e| e == "yml") {
            files.push(path);
        }
    }
    let _ = root;
    Ok(())
}

fn scalar_string(value: &Yaml) -> Option<String> {
    match value {
        Yaml::String(s) => Some(s.clone()),
        Yaml::Number(n) => Some(n.to_string()),
        Yaml::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The sliver of ERB the fixtures use.
struct Erb<'a> {
    options: &'a Options,
    locals: HashMap<String, String>,
    digests: HashMap<String, String>,
}

impl<'a> Erb<'a> {
    fn new(options: &'a Options) -> Self {
        Self {
            options,
            locals: HashMap::new(),
            digests: HashMap::new(),
        }
    }

    fn render(&mut self, source: &str) -> Result<String> {
        let mut out = String::new();
        let mut rest = source;
        while let Some(start) = rest.find("<%") {
            out.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let end = after
                .find("%>")
                .ok_or_else(|| Error::Other("unterminated ERB tag".into()))?;
            let (output, code) = match after.strip_prefix('=') {
                Some(code) => (true, &code[..end - 1]),
                None => (false, &after[..end]),
            };
            let value = self.evaluate(code.trim())?;
            if output {
                out.push_str(&value);
            }
            rest = &after[end + 2..];
            // `<% %>` on its own line leaves the newline, as ERB without trim mode does.
        }
        out.push_str(rest);
        Ok(out)
    }

    fn evaluate(&mut self, code: &str) -> Result<String> {
        if let Some((name, expression)) = code.split_once(" = ") {
            let value = self.evaluate(expression.trim())?;
            self.locals.insert(name.trim().to_string(), value);
            return Ok(String::new());
        }
        if let Some(value) = self.locals.get(code) {
            return Ok(value.clone());
        }
        if let Some(password) = code
            .strip_prefix("BCrypt::Password.create(")
            .and_then(|c| c.strip_suffix(')'))
        {
            let password = password.trim().trim_matches('"').to_string();
            if let Some(digest) = self.digests.get(&password) {
                return Ok(digest.clone());
            }
            let digest = user::password_digest(&password, self.options.bcrypt_cost)?;
            self.digests.insert(password, digest.clone());
            return Ok(digest);
        }
        if code == "User.generate_bot_token" {
            return Ok(user::generate_bot_token());
        }
        if let Some(duration) = parse_ago(code) {
            // `TimeWithZone#to_s` in UTC: whole seconds.
            let at = Timestamp::from_second(self.options.now.as_second()).ago(duration);
            return Ok(format!("{} UTC", at.to_db()));
        }
        Err(Error::Other(format!("unsupported ERB in fixture: {code}")))
    }
}

/// `1.hour.ago`, `36.minutes.ago`, `2.days.ago`...
fn parse_ago(code: &str) -> Option<SignedDuration> {
    let mut parts = code.split('.');
    let n: i64 = parts.next()?.trim().parse().ok()?;
    let unit = parts.next()?;
    if parts.next()? != "ago" || parts.next().is_some() {
        return None;
    }
    let seconds = match unit.trim_end_matches('s') {
        "second" => 1,
        "minute" => 60,
        "hour" => 3600,
        "day" => 86_400,
        "week" => 604_800,
        _ => return None,
    };
    Some(SignedDuration::from_secs(n * seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identify_matches_rails() {
        // ActiveRecord::FixtureSet.identify, from the reference app.
        assert_eq!(identify("david"), 127326141);
        assert_eq!(identify("signal"), 873240054);
        assert_eq!(identify("designers"), 654632876);
    }

    #[test]
    fn table_names() {
        let dir = Path::new("/f");
        assert_eq!(
            table_name(dir, Path::new("/f/action_text/rich_texts.yml")),
            "action_text_rich_texts"
        );
        assert_eq!(
            table_name(dir, Path::new("/f/push/subscriptions.yml")),
            "push_subscriptions"
        );
        assert_eq!(table_name(dir, Path::new("/f/users.yml")), "users");
    }

    #[test]
    fn erb_ago_renders_whole_seconds() {
        let options = Options {
            now: Timestamp::parse_db("2026-09-26 12:24:38.211309").unwrap(),
            bcrypt_cost: 4,
        };
        let mut erb = Erb::new(&options);
        assert_eq!(
            erb.render("created_at: <%= 1.hour.ago %>").unwrap(),
            "created_at: 2026-09-26 11:24:38 UTC"
        );
        assert_eq!(
            erb.render("<% x = 5.minutes.ago %>\na: <%= x %>").unwrap(),
            "\na: 2026-09-26 12:19:38 UTC"
        );
    }
}
