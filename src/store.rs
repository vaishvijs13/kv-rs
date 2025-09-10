use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use crate::{
    aof::{Aof, LogEntry},
    error::{RedisError, Response},
    types::{Entry, RedisValue},
};

#[derive(Clone)]
pub struct Store {
    inner: Arc<RwLock<HashMap<String, Entry>>>,
    aof: Option<Aof>,
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
                        map.insert(e.key, Entry::string(val, expires_at));
                    }
                }
                "del" => { map.remove(&e.key); }
                _ => {}
            }
        }
    }

    pub fn set(&self, key: String, value: String, ttl_secs: Option<u64>) -> Response {
        let expires_at = ttl_secs.map(|s| SystemTime::now() + Duration::from_secs(s));
        {
            let mut map = self.inner.write().unwrap();
            map.insert(key.clone(), Entry::string(value.clone(), expires_at));
        }
        
        if let Some(aof) = &self.aof {
            aof.log(LogEntry {
                op: "set".into(),
                key,
                value: Some(value),
                expires_at_ms: expires_at.map(|t| t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64),
            });
        }
        "OK".into()
    }

    pub fn get(&self, key: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get(key) {
            if entry.is_expired() {
                map.remove(key);
                return Response::Nil;
            }
            if let Some(string_val) = entry.value.as_string() {
                return Response::BulkString(Some(string_val.clone()));
            }
            return RedisError::InvalidType(format!("WRONGTYPE Operation against a key holding the wrong kind of value")).into();
        }
        Response::Nil
    }

    pub fn del(&self, key: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        let removed = if let Some(entry) = map.get(key) {
            if entry.is_expired() {
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
        Response::Integer(removed)
    }

    pub fn exists(&self, key: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get(key) {
            if entry.is_expired() {
                map.remove(key);
                Response::Integer(0)
            } else {
                Response::Integer(1)
            }
        } else { 
            Response::Integer(0) 
        }
    }

    pub fn ttl(&self, key: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get(key) {
            if entry.is_expired() {
                map.remove(key);
                return Response::Integer(-2); // not found
            }
            match entry.expires_at {
                Some(exp) => {
                    let now = SystemTime::now();
                    let rem = exp.duration_since(now).unwrap_or_default().as_secs() as i64;
                    Response::Integer(rem)
                }
                None => Response::Integer(-1), // no TTL
            }
        } else { 
            Response::Integer(-2) 
        }
    }

    pub fn keys_with_prefix(&self, prefix: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        Self::sweep_locked(&mut map);
        let keys: Vec<String> = map.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        
        if keys.is_empty() {
            Response::Array(vec![])
        } else {
            Response::Array(keys.into_iter().map(|k| Response::BulkString(Some(k))).collect())
        }
    }

    pub fn incr(&self, key: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get_mut(key) {
            if entry.is_expired() {
                map.remove(key);
                let new = 1i64;
                map.insert(key.to_string(), Entry::string(new.to_string(), None));
                self.log_set(key.to_string(), new.to_string(), None);
                return Response::Integer(new);
            } else {
                if let Some(string_val) = entry.value.as_string() {
                    match string_val.parse::<i64>() {
                        Ok(cur) => {
                            let new = cur + 1;
                            entry.value = RedisValue::String(new.to_string());
                            self.log_set(key.to_string(), new.to_string(), entry.expires_at);
                            return Response::Integer(new);
                        }
                        Err(_) => {
                            return RedisError::NotInteger(string_val.clone()).into();
                        }
                    }
                } else {
                    return RedisError::InvalidType("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()).into();
                }
            }
        } else {
            let new = 1i64;
            map.insert(key.to_string(), Entry::string(new.to_string(), None));
            self.log_set(key.to_string(), new.to_string(), None);
            Response::Integer(new)
        }
    }

    // list ops
    pub fn lpush(&self, key: &str, values: Vec<String>) -> Response {
        let mut map = self.inner.write().unwrap();
        let entry = map.entry(key.to_string()).or_insert_with(|| Entry::list(None));
        
        if entry.is_expired() {
            *entry = Entry::list(None);
        }
        
        if let Some(list) = entry.value.as_list_mut() {
            for value in values.iter().rev() {
                list.push_front(value.clone());
            }
            Response::Integer(list.len() as i64)
        } else {
            RedisError::InvalidType("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()).into()
        }
    }

    pub fn lpop(&self, key: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get_mut(key) {
            if entry.is_expired() {
                map.remove(key);
                return Response::Nil;
            }
            if let Some(list) = entry.value.as_list_mut() {
                if let Some(value) = list.pop_front() {
                    if list.is_empty() {
                        map.remove(key);
                    }
                    Response::BulkString(Some(value))
                } else {
                    Response::Nil
                }
            } else {
                RedisError::InvalidType("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()).into()
            }
        } else {
            Response::Nil
        }
    }

    pub fn llen(&self, key: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get(key) {
            if entry.is_expired() {
                map.remove(key);
                return Response::Integer(0);
            }
            if let RedisValue::List(list) = &entry.value {
                Response::Integer(list.len() as i64)
            } else {
                RedisError::InvalidType("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()).into()
            }
        } else {
            Response::Integer(0)
        }
    }

    // set ops  
    pub fn sadd(&self, key: &str, members: Vec<String>) -> Response {
        let mut map = self.inner.write().unwrap();
        let entry = map.entry(key.to_string()).or_insert_with(|| Entry::set(None));
        
        if entry.is_expired() {
            *entry = Entry::set(None);
        }
        
        if let Some(set) = entry.value.as_set_mut() {
            let mut added = 0;
            for member in members {
                if set.insert(member) {
                    added += 1;
                }
            }
            Response::Integer(added)
        } else {
            RedisError::InvalidType("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()).into()
        }
    }

    pub fn srem(&self, key: &str, members: Vec<String>) -> Response {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get_mut(key) {
            if entry.is_expired() {
                map.remove(key);
                return Response::Integer(0);
            }
            if let Some(set) = entry.value.as_set_mut() {
                let mut removed = 0;
                for member in members {
                    if set.remove(&member) {
                        removed += 1;
                    }
                }
                if set.is_empty() {
                    map.remove(key);
                }
                Response::Integer(removed)
            } else {
                RedisError::InvalidType("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()).into()
            }
        } else {
            Response::Integer(0)
        }
    }

    pub fn scard(&self, key: &str) -> Response {
        let mut map = self.inner.write().unwrap();
        if let Some(entry) = map.get(key) {
            if entry.is_expired() {
                map.remove(key);
                return Response::Integer(0);
            }
            if let RedisValue::Set(set) = &entry.value {
                Response::Integer(set.len() as i64)
            } else {
                RedisError::InvalidType("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()).into()
            }
        } else {
            Response::Integer(0)
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

    fn sweep_locked(map: &mut HashMap<String, Entry>) {
        let keys_to_remove: Vec<String> = map.iter()
            .filter_map(|(k, v)| if v.is_expired() { Some(k.clone()) } else { None })
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
