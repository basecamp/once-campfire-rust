//! The request as a Rails controller sees it: method after `_method` override, URL pieces with
//! proxy-derived host and protocol, `remote_ip`, and the raw body.

use std::fmt::Write;
use std::net::IpAddr;

use axum::body::Bytes;
use axum::http::{HeaderMap, Method, Uri};

use crate::{Error, Result};

/// How to read proxy headers (`ActionDispatch::RemoteIp`, `ActionDispatch::AssumeSSL`).
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// `config.action_dispatch.trusted_proxies`; Rails' `TRUSTED_PROXIES` by default.
    pub trusted_proxies: Vec<IpNet>,
    /// `config.action_dispatch.ip_spoofing_check`.
    pub ip_spoofing_check: bool,
    /// `config.assume_ssl`: treat every request as HTTPS (TLS terminates at the proxy).
    pub assume_ssl: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self { trusted_proxies: default_trusted_proxies(), ip_spoofing_check: true, assume_ssl: false }
    }
}

/// `ActionDispatch::RemoteIp::TRUSTED_PROXIES`.
pub fn default_trusted_proxies() -> Vec<IpNet> {
    ["127.0.0.0/8", "::1/128", "fc00::/7", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "fe80::/10"]
        .iter()
        .map(|s| s.parse().unwrap())
        .collect()
}

/// An IP network (`IPAddr.new("10.0.0.0/8")`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNet {
    addr: IpAddr,
    prefix: u8,
}

impl IpNet {
    pub fn contains(&self, ip: &IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => mask_eq(&net.octets(), &ip.octets(), self.prefix),
            (IpAddr::V6(net), IpAddr::V6(ip)) => mask_eq(&net.octets(), &ip.octets(), self.prefix),
            _ => false,
        }
    }
}

fn mask_eq(a: &[u8], b: &[u8], prefix: u8) -> bool {
    let full = (prefix / 8) as usize;
    if a[..full] != b[..full] {
        return false;
    }
    let rest = prefix % 8;
    rest == 0 || {
        let mask = 0xffu8 << (8 - rest);
        a[full] & mask == b[full] & mask
    }
}

impl std::str::FromStr for IpNet {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (addr, prefix) = match s.split_once('/') {
            Some((addr, prefix)) => (addr, Some(prefix)),
            None => (s, None),
        };
        let addr: IpAddr = addr.parse().map_err(|_| format!("invalid IP {s:?}"))?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(p) => p.parse::<u8>().ok().filter(|p| *p <= max).ok_or_else(|| format!("invalid prefix {s:?}"))?,
            None => max,
        };
        Ok(Self { addr, prefix })
    }
}

#[derive(Debug)]
pub struct Request {
    /// The method the app sees (after `Rack::MethodOverride`).
    pub method: Method,
    /// The method on the wire (`request.method` in Rails).
    pub original_method: Method,
    pub uri: Uri,
    pub headers: HeaderMap,
    /// The TCP peer (`REMOTE_ADDR`).
    pub peer: Option<IpAddr>,
    body: Bytes,
    ssl: bool,
    remote_ip: std::result::Result<String, ()>,
}

impl Request {
    pub fn new(
        method: Method,
        original_method: Method,
        uri: Uri,
        headers: HeaderMap,
        peer: Option<IpAddr>,
        body: Bytes,
        proxy: &ProxyConfig,
    ) -> Self {
        let ssl = proxy.assume_ssl || scheme_is_https(&headers, &uri);
        let remote_ip = calculate_remote_ip(&headers, peer, proxy);
        Self { method, original_method, uri, headers, peer, body, ssl, remote_ip }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    pub fn is_get(&self) -> bool {
        self.method == Method::GET
    }

    pub fn is_head(&self) -> bool {
        self.method == Method::HEAD
    }

    pub fn is_post(&self) -> bool {
        self.method == Method::POST
    }

    /// `request.xhr?`
    pub fn is_xhr(&self) -> bool {
        self.header("x-requested-with").is_some_and(|v| v.to_ascii_lowercase().contains("xmlhttprequest"))
    }

    pub fn path(&self) -> &str {
        self.uri.path()
    }

    pub fn query_string(&self) -> &str {
        self.uri.query().unwrap_or("")
    }

    /// `request.fullpath`
    pub fn fullpath(&self) -> String {
        match self.uri.query() {
            Some(q) if !q.is_empty() => format!("{}?{q}", self.uri.path()),
            _ => self.uri.path().to_string(),
        }
    }

    pub fn is_ssl(&self) -> bool {
        self.ssl
    }

    /// `request.protocol`: `"https://"` or `"http://"`.
    pub fn protocol(&self) -> &'static str {
        if self.ssl { "https://" } else { "http://" }
    }

    fn raw_host_with_port(&self) -> String {
        if let Some(forwarded) = self.header("x-forwarded-host").filter(|h| !h.trim().is_empty()) {
            return forwarded.split(',').next_back().unwrap_or("").trim_start().to_string();
        }
        self.header("host")
            .map(str::to_string)
            .or_else(|| self.uri.authority().map(|a| a.to_string()))
            .unwrap_or_else(|| "localhost".into())
    }

    /// `request.host` (X-Forwarded-Host aware, port stripped).
    pub fn host(&self) -> String {
        let raw = self.raw_host_with_port();
        match raw.rsplit_once(':') {
            Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => host.to_string(),
            _ => raw,
        }
    }

    pub fn port(&self) -> u16 {
        let raw = self.raw_host_with_port();
        match raw.rsplit_once(':') {
            Some((_, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => port.parse().unwrap_or(self.standard_port()),
            _ => self.standard_port(),
        }
    }

    fn standard_port(&self) -> u16 {
        if self.ssl { 443 } else { 80 }
    }

    /// `request.host_with_port`: the port only when it isn't the scheme's default.
    pub fn host_with_port(&self) -> String {
        let mut authority = self.raw_host_with_port();
        let Some((host, port)) = authority.rsplit_once(':') else { return authority };
        if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
            return authority;
        }
        let port = port.parse::<u16>().unwrap_or(self.standard_port());
        let host_len = host.len();
        authority.truncate(host_len);
        if port != self.standard_port() {
            // A canonical decimal port never needs more space than the original digits.
            write!(authority, ":{port}").unwrap();
        }
        authority
    }

    /// `request.base_url`, e.g. `https://campfire.example.com`.
    pub fn base_url(&self) -> String {
        format!("{}{}", self.protocol(), self.host_with_port())
    }

    /// `request.url`
    pub fn url(&self) -> String {
        format!("{}{}", self.base_url(), self.fullpath())
    }

    pub fn origin(&self) -> Option<&str> {
        self.header("origin")
    }

    pub fn referer(&self) -> Option<&str> {
        self.header("referer")
    }

    pub fn user_agent(&self) -> Option<&str> {
        self.header("user-agent")
    }

    pub fn content_type(&self) -> Option<&str> {
        self.header("content-type").filter(|ct| !ct.is_empty())
    }

    /// The `type/subtype` part of `Content-Type`, lowercased (`Rack::MediaType.type`).
    pub fn media_type(&self) -> Option<String> {
        media_type(self.content_type())
    }

    /// `request.raw_post` / `request.body.read`. Empty for multipart bodies, which are spooled
    /// into params instead.
    pub fn raw_post(&self) -> &Bytes {
        &self.body
    }

    /// `request.remote_ip`, per `ActionDispatch::RemoteIp::GetIp`.
    pub fn remote_ip(&self) -> Result<&str> {
        self.remote_ip.as_deref().map_err(|_| Error::IpSpoofAttack)
    }
}

pub fn media_type(content_type: Option<&str>) -> Option<String> {
    let ct = content_type?;
    let base = ct.split([';', ',']).next()?.trim().to_ascii_lowercase();
    if base.is_empty() { None } else { Some(base) }
}

/// `Rack::Request#scheme` → `ssl?`.
pub fn scheme_is_https(headers: &HeaderMap, uri: &Uri) -> bool {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if header("x-forwarded-ssl") == Some("on") {
        return true;
    }
    if let Some(forwarded) = header("forwarded")
        && let Some(proto) = forwarded_values(forwarded, "proto").last()
        && ["https", "http", "wss", "ws"].contains(&proto.as_str())
    {
        return proto == "https" || proto == "wss";
    }
    for name in ["x-forwarded-proto", "x-forwarded-scheme"] {
        if let Some(value) = header(name)
            && let Some(scheme) = split_header(value).rev().find(|s| ["https", "http", "wss", "ws"].contains(s))
        {
            return scheme == "https" || scheme == "wss";
        }
    }
    uri.scheme_str() == Some("https")
}

fn split_header(value: &str) -> impl DoubleEndedIterator<Item = &str> {
    value.trim().split([',', ' ', '\t']).filter(|s| !s.is_empty())
}

/// `Rack::Utils.forwarded_values(header)[param]`, for the simple unquoted and quoted forms.
fn forwarded_values(header: &str, param: &str) -> Vec<String> {
    let mut values = Vec::new();
    for element in header.split([',', ';']) {
        if let Some((name, value)) = element.split_once('=')
            && name.trim().eq_ignore_ascii_case(param)
        {
            values.push(value.trim().trim_matches('"').to_string());
        }
    }
    values
}

/// Strip a port and IPv6 brackets: `Rack::Request#split_authority(...)[1]`.
fn authority_address(authority: &str) -> String {
    let authority = authority.trim();
    if let Some(rest) = authority.strip_prefix('[') {
        return rest.split(']').next().unwrap_or("").to_string();
    }
    if authority.parse::<IpAddr>().is_ok() {
        return authority.to_string();
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if port.bytes().all(|b| b.is_ascii_digit()) && !host.contains(':') => host.to_string(),
        _ => authority.to_string(),
    }
}

fn sanitize_ips<'a>(ips: impl Iterator<Item = &'a str>) -> Vec<IpAddr> {
    ips.filter_map(|ip| ip.trim_matches(['[', ']']).parse::<IpAddr>().ok()).collect()
}

fn forwarded_for(headers: &HeaderMap) -> Option<Vec<String>> {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(forwarded) = header("forwarded") {
        let values = forwarded_values(forwarded, "for");
        if !values.is_empty() {
            return Some(values.iter().map(|v| authority_address(v)).collect());
        }
    }
    header("x-forwarded-for").map(|value| split_header(value).map(authority_address).collect())
}

/// `ActionDispatch::RemoteIp::GetIp#calculate_ip`.
fn calculate_remote_ip(headers: &HeaderMap, peer: Option<IpAddr>, proxy: &ProxyConfig) -> std::result::Result<String, ()> {
    let remote_addr = peer;
    let client_ip_header = headers.get("client-ip").and_then(|v| v.to_str().ok());
    let mut client_ips = sanitize_ips(client_ip_header.map(|h| h.trim().split([',', ' ', '\t'])).into_iter().flatten());
    client_ips.reverse();
    let forwarded = forwarded_for(headers).unwrap_or_default();
    let mut forwarded_ips = sanitize_ips(forwarded.iter().map(String::as_str));
    forwarded_ips.reverse();

    if proxy.ip_spoofing_check
        && let (Some(client), Some(_)) = (client_ips.last(), forwarded_ips.last())
        && !forwarded_ips.contains(client)
    {
        return Err(());
    }

    let ips: Vec<IpAddr> = forwarded_ips.into_iter().chain(client_ips).collect();
    let trusted = |ip: &IpAddr| proxy.trusted_proxies.iter().any(|net| net.contains(ip));
    let chosen = ips.iter().chain(remote_addr.iter()).find(|ip| !trusted(ip)).or(ips.last()).or(remote_addr.as_ref());
    Ok(chosen.map(|ip| ip.to_string()).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(headers: &[(&str, &str)], peer: &str, proxy: &ProxyConfig) -> Request {
        let mut map = HeaderMap::new();
        for (k, v) in headers {
            map.append(axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(), v.parse().unwrap());
        }
        Request::new(Method::GET, Method::GET, "/rooms/1?x=1".parse().unwrap(), map, Some(peer.parse().unwrap()), Bytes::new(), proxy)
    }

    #[test]
    fn remote_ip_skips_trusted_proxies() {
        let proxy = ProxyConfig::default();
        let r = request(&[("x-forwarded-for", "203.0.113.9, 10.0.0.2")], "127.0.0.1", &proxy);
        assert_eq!(r.remote_ip().unwrap(), "203.0.113.9");
        let r = request(&[], "198.51.100.7", &proxy);
        assert_eq!(r.remote_ip().unwrap(), "198.51.100.7");
        // As in Rails, a forwarded address is believed even from a public peer: `remote_ip` is
        // only as trustworthy as the proxy in front.
        let r = request(&[("x-forwarded-for", "203.0.113.9")], "198.51.100.7", &proxy);
        assert_eq!(r.remote_ip().unwrap(), "203.0.113.9");
        let r = request(&[("x-forwarded-for", "10.0.0.3")], "127.0.0.1", &proxy);
        assert_eq!(r.remote_ip().unwrap(), "10.0.0.3");
        let r = request(&[("forwarded", "for=\"[2001:db8::1]:4711\"")], "::1", &proxy);
        assert_eq!(r.remote_ip().unwrap(), "2001:db8::1");
    }

    #[test]
    fn remote_ip_spoofing_check() {
        let proxy = ProxyConfig::default();
        let r = request(&[("client-ip", "1.1.1.1"), ("x-forwarded-for", "2.2.2.2")], "127.0.0.1", &proxy);
        assert!(matches!(r.remote_ip(), Err(Error::IpSpoofAttack)));
        let r = request(&[("client-ip", "2.2.2.2"), ("x-forwarded-for", "2.2.2.2")], "127.0.0.1", &proxy);
        assert_eq!(r.remote_ip().unwrap(), "2.2.2.2");
    }

    #[test]
    fn host_and_protocol() {
        let proxy = ProxyConfig::default();
        let r = request(&[("host", "chat.example.com:3000")], "127.0.0.1", &proxy);
        assert_eq!(r.host(), "chat.example.com");
        assert_eq!(r.port(), 3000);
        assert_eq!(r.base_url(), "http://chat.example.com:3000");
        assert_eq!(r.url(), "http://chat.example.com:3000/rooms/1?x=1");

        let r = request(
            &[("host", "internal:80"), ("x-forwarded-host", "a.example, chat.example.com"), ("x-forwarded-proto", "https")],
            "127.0.0.1",
            &proxy,
        );
        assert_eq!(r.host(), "chat.example.com");
        assert!(r.is_ssl());
        assert_eq!(r.base_url(), "https://chat.example.com");

        let assume = ProxyConfig { assume_ssl: true, ..ProxyConfig::default() };
        let r = request(&[("host", "chat.example.com")], "127.0.0.1", &assume);
        assert_eq!(r.base_url(), "https://chat.example.com");
    }

    #[test]
    fn host_with_port_preserves_authority_normalization() {
        for (authority, http, https) in [
            ("chat.example", "chat.example", "chat.example"),
            ("chat.example:80", "chat.example", "chat.example:80"),
            ("chat.example:443", "chat.example:443", "chat.example"),
            ("chat.example:00081", "chat.example:81", "chat.example:81"),
            ("chat.example:65536", "chat.example", "chat.example"),
            ("chat.example:99999999999999999999", "chat.example", "chat.example"),
            ("chat.example:", "chat.example:", "chat.example:"),
            ("chat.example:abc", "chat.example:abc", "chat.example:abc"),
            ("[2001:db8::1]", "[2001:db8::1]", "[2001:db8::1]"),
            ("[2001:db8::1]:00443", "[2001:db8::1]:443", "[2001:db8::1]"),
            ("[2001:db8::1]:3000", "[2001:db8::1]:3000", "[2001:db8::1]:3000"),
        ] {
            for (assume_ssl, expected) in [(false, http), (true, https)] {
                let proxy = ProxyConfig { assume_ssl, ..ProxyConfig::default() };
                let direct = request(&[("host", authority)], "127.0.0.1", &proxy);
                assert_eq!(direct.host_with_port(), expected, "{authority}, ssl={assume_ssl}");
                let forwarded = format!("ignored.example, {authority}");
                let proxied = request(&[("host", "internal:3000"), ("x-forwarded-host", &forwarded)], "127.0.0.1", &proxy);
                assert_eq!(proxied.host_with_port(), expected, "forwarded {authority}, ssl={assume_ssl}");
            }
        }
    }

    #[test]
    fn ip_nets() {
        let net: IpNet = "172.16.0.0/12".parse().unwrap();
        assert!(net.contains(&"172.31.255.1".parse().unwrap()));
        assert!(!net.contains(&"172.32.0.1".parse().unwrap()));
        let net: IpNet = "fc00::/7".parse().unwrap();
        assert!(net.contains(&"fd12::1".parse().unwrap()));
    }
}
