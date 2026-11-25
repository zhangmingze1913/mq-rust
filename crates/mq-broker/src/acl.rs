//! Minimal RocketMQ ACL policy file support.
//!
//! Schema (compatible subset):
//! {
//!   "accounts": [
//!     {"accessKey":"key","secretKey":"secret",
//!      "permissions":[{"topic":"MQ_*","perm":"WRITE"}]}
//!   ]
//! }

use serde::Deserialize;
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Debug, Deserialize)]
pub struct AclFile {
    #[serde(default)]
    pub accounts: Vec<AclAccount>,
}

#[derive(Debug, Deserialize)]
pub struct AclAccount {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    #[serde(default)]
    pub permissions: Vec<AclPermission>,
    #[serde(default, rename = "whiteRemoteAddress")]
    pub white_remote_address: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AclPermission {
    #[serde(default = "star")]
    pub topic: String,
    #[serde(default = "star")]
    pub group: String,
    #[serde(default = "deny")]
    pub perm: String,
}

fn star() -> String {
    "*".to_string()
}

fn deny() -> String {
    "DENY".to_string()
}

#[derive(Debug, Default)]
pub struct AclPolicy {
    accounts: HashMap<String, AclAccount>,
}

impl AclPolicy {
    pub fn load(path: &str) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let file: AclFile = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        let mut accounts = HashMap::new();
        for account in file.accounts {
            accounts.insert(account.access_key.clone(), account);
        }
        Ok(Self { accounts })
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    pub fn secret(&self, access_key: &str) -> Option<&str> {
        self.accounts
            .get(access_key)
            .map(|account| account.secret_key.as_str())
    }

    /// `action` is "READ" or "WRITE". DENY wins over any allow.
    pub fn allows(&self, access_key: &str, topic: &str, action: &str, peer: &str) -> bool {
        let Some(account) = self.accounts.get(access_key) else {
            return false;
        };
        if let Some(white) = &account.white_remote_address {
            if !ip_matches(white, peer) {
                return false;
            }
        }
        let mut allowed = false;
        for permission in &account.permissions {
            if !topic_matches(&permission.topic, topic) {
                continue;
            }
            let perm = permission.perm.to_ascii_uppercase();
            if perm == "DENY" {
                return false;
            }
            if perm == "ANY" || perm == "ALL" || perm == action {
                allowed = true;
            }
        }
        allowed
    }

    /// Group-scoped check (RocketMQ ACL group permissions).
    pub fn allows_group(
        &self,
        access_key: &str,
        topic: &str,
        group: &str,
        action: &str,
        peer: &str,
    ) -> bool {
        let Some(account) = self.accounts.get(access_key) else {
            return false;
        };
        if let Some(white) = &account.white_remote_address {
            if !ip_matches(white, peer) {
                return false;
            }
        }
        let mut allowed = false;
        for permission in &account.permissions {
            if !topic_matches(&permission.topic, topic) || !group_matches(&permission.group, group)
            {
                continue;
            }
            let perm = permission.perm.to_ascii_uppercase();
            if perm == "DENY" {
                return false;
            }
            if perm == "ANY" || perm == "ALL" || perm == action {
                allowed = true;
            }
        }
        allowed
    }
}

fn group_matches(pattern: &str, group: &str) -> bool {
    topic_matches(pattern, group)
}

fn topic_matches(pattern: &str, topic: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return topic.starts_with(prefix);
    }
    pattern == topic
}

fn ip_matches(pattern: &str, peer: &str) -> bool {
    if pattern == "*" || pattern.is_empty() {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return peer.starts_with(prefix);
    }
    if let Some((network, bits)) = pattern.split_once('/') {
        let Ok(bits) = bits.parse::<u32>() else {
            return false;
        };
        if let (Ok(network), Ok(peer)) = (network.parse::<Ipv4Addr>(), peer.parse::<Ipv4Addr>()) {
            if bits > 32 {
                return false;
            }
            let mask = if bits == 0 {
                0
            } else {
                u32::MAX << (32 - bits)
            };
            return (u32::from(network) & mask) == (u32::from(peer) & mask);
        }
        if let (Ok(network), Ok(peer)) = (network.parse::<Ipv6Addr>(), peer.parse::<Ipv6Addr>()) {
            if bits > 128 {
                return false;
            }
            let mask = if bits == 0 {
                0
            } else {
                u128::MAX << (128 - bits)
            };
            return (u128::from(network) & mask) == (u128::from(peer) & mask);
        }
        return false;
    }
    pattern == peer
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> AclPolicy {
        let mut accounts = HashMap::new();
        accounts.insert(
            "key".to_string(),
            AclAccount {
                access_key: "key".into(),
                secret_key: "secret".into(),
                permissions: vec![
                    AclPermission {
                        topic: "MQ_*".into(),
                        group: "G_*".into(),
                        perm: "WRITE".into(),
                    },
                    AclPermission {
                        topic: "MQ_DENY".into(),
                        group: "*".into(),
                        perm: "DENY".into(),
                    },
                ],
                white_remote_address: Some("127.0.0.1".into()),
            },
        );
        AclPolicy { accounts }
    }

    #[test]
    fn permission_matching() {
        let p = policy();
        assert_eq!(p.secret("key"), Some("secret"));
        assert!(p.allows("key", "MQ_ALLOW", "WRITE", "127.0.0.1"));
        assert!(!p.allows("key", "MQ_ALLOW", "READ", "127.0.0.1"));
        assert!(!p.allows("key", "MQ_DENY", "WRITE", "127.0.0.1"));
        assert!(!p.allows("unknown", "MQ_ALLOW", "WRITE", "127.0.0.1"));
        assert!(!p.allows("key", "MQ_ALLOW", "WRITE", "10.0.0.1"));
        assert!(p.allows_group("key", "MQ_ALLOW", "G_READ", "WRITE", "127.0.0.1"));
        assert!(!p.allows_group("key", "MQ_ALLOW", "OTHER", "WRITE", "127.0.0.1"));
        assert!(!p.allows_group("key", "MQ_ALLOW", "G_READ", "READ", "127.0.0.1"));
    }

    #[test]
    fn ip_cidr_matching() {
        assert!(ip_matches("127.0.0.0/8", "127.0.0.1"));
        assert!(ip_matches("10.0.0.0/8", "10.1.2.3"));
        assert!(!ip_matches("10.0.0.0/8", "11.0.0.1"));
        assert!(ip_matches("2001:db8::/32", "2001:db8::1"));
        assert!(!ip_matches("2001:db8::/32", "2001:db9::1"));
        assert!(!ip_matches("127.0.0.0/33", "127.0.0.1"));
    }
}
