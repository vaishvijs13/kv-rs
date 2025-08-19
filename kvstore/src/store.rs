use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use crate::aof::{Aof, LogEntry};

#[derive(Clone)]
pub struct Store {
    inner: Arc<RwLock<HashMap<String, Entry>>>,
    aof: Option<Aof>,
}

#[derive(Clone, Debug)]
struct Entry {
    value: String,
    expires_at: Option<SystemTime>,
}

impl Store {
    pub fn new(aof: Option<Aof>) -> Self {
        Store {
            inner: Arc::new(RwLock::new(HashMap::new())),
            aof,
        }
    }

    pub fn load_from_aof(&self, entries: Vec<LogEntry>) {
        let mut map = self.inner.write().unwrap();
        for e in entries {
            match e.op.as_str() {
                "set" => {
                    let expires_at = e.expires_at_ms.map(|ms| UNIX_EPOCH + Duration::from_millis(ms as u64));
                    if let Some(val) = e.value {
                        map.insert(e.key, Entry { value: val, expires_at });
                    }
                }
                "del" => { map.remove(&e.key); }
                _ => {}
            }
        }
    }

    pub fn set(&self, key: String, value: String, ttl_secs: Option<u64>) -> String {
        let expires_at = ttl_secs.map(|s| SystemTime::now() + Duration::from_secs(s));
        {
            let mut map = self.inner.write().unwrap();
            map.insert(key.clone(), Entry { value: value.clone(), expires_at });
        }
        if let Some(aof) = &self.aof {
            aof.log(LogEntry {
                op: "set".into(),
                key,
                value: Some(value),
                expires_at_ms: expires_at.map(|t| t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64),
            });
        }
        "OK".to_string()
    }

    pub fn get(&self, key: &str) -> String {
        let mut map = self.inner.write().unwrap();
        if let Some(ent) = map.get(key) {
            if Self::expired(ent) {
                map.remove(key);
                return "(nil)".to_string();
            }
            return ent.value.clone();
        }
        "(nil)".to_string()
    }

    pub fn del(&self, key: &str) -> String {
        let mut map = self.inner.write().unwrap();
        let removed = if let Some(ent) = map.get(key) {
            if Self::expired(ent) {
                map.remove(key);
                0
            } else {
                map.remove(key);
                1
            }
        } else { 0 };

        if removed == 1 {
            if let Some(aof) = &self.aof {
                aof.log(LogEntry {
                    op: "del".into(),
                    key: key.to_string(),
                    value: None,
                    expires_at_ms: None,
                });
            }
        }
        removed.to_string()
    }

    pub fn exists(&self, key: &str) -> String {
        let mut map = self.inner.write().unwrap();
        if let Some(ent) = map.get(key) {
            if Self::expired(ent) {
                map.remove(key);
                "0".into()
            } else {
                "1".into()
            }
        } else { "0".into() }
    }

    pub fn ttl(&self, key: &str) -> String {
        let mut map = self.inner.write().unwrap();
        if let Some(ent) = map.get(key) {
            if Self::expired(ent) {
                map.remove(key);
                return "-2".to_string(); // not found
            }
            match ent.expires_at {
                Some(exp) => {
                    let now = SystemTime::now();
                    let rem = exp.duration_since(now).unwrap_or_default().as_secs() as i64;
                    rem.to_string()
                }
                None => "-1".into(), // no TTL
            }
        } else { "-2".into() }
    }

    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        let mut map = self.inner.write().unwrap();
        Self::sweep_locked(&mut map);
        map.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    pub fn incr(&self, key: &str) -> Result<String, String> {
        let mut map = self.inner.write().unwrap();
        if let Some(ent) = map.get_mut(key) {
            if Self::expired(ent) {
                map.remove(key);
                let new = 1i64;
                map.insert(key.to_string(), Entry { value: new.to_string(), expires_at: None });
                self.log_set(key.to_string(), new.to_string(), None);
                return Ok(new.to_string());
            } else {
                let cur: i64 = ent.value.parse().map_err(|_| "ERR value is not an integer".to_string())?;
                let new = cur + 1;
                ent.value = new.to_string();
                self.log_set(key.to_string(), ent.value.clone(), ent.expires_at);
                return Ok(new.to_string());
            }
        } else {
            let new = 1i64;
            map.insert(key.to_string(), Entry { value: new.to_string(), expires_at: None });
            self.log_set(key.to_string(), new.to_string(), None);
            Ok(new.to_string())
        }
    }

    fn log_set(&self, key: String, value: String, exp: Option<SystemTime>) {
        if let Some(aof) = &self.aof {
            aof.log(LogEntry {
                op: "set".into(),
                key,
                value: Some(value),
                expires_at_ms: exp.map(|t| t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64),
            });
        }
    }

    fn expired(entry: &Entry) -> bool {
        if let Some(exp) = entry.expires_at {
            SystemTime::now() > exp
        } else { false }
    }

    fn sweep_locked(map: &mut HashMap<String, Entry>) {
        let keys_to_remove: Vec<String> = map.iter()
            .filter_map(|(k, v)| if Self::expired(v) { Some(k.clone()) } else { None })
            .collect();
        for k in keys_to_remove {
            map.remove(&k);
        }
    }

    pub async fn start_sweeper(self, period_secs: u64) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(period_secs));
        loop {
            interval.tick().await;
            let mut map = self.inner.write().unwrap();
            Self::sweep_locked(&mut map);
        }
    }
}
