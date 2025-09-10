use std::collections::{HashMap, HashSet, VecDeque};
use std::time::SystemTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisValue {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
}

impl RedisValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Set(_) => "set", 
            RedisValue::Hash(_) => "hash",
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            RedisValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<String>> {
        match self {
            RedisValue::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut HashSet<String>> {
        match self {
            RedisValue::Set(set) => Some(set),
            _ => None,
        }
    }

    pub fn as_hash_mut(&mut self) -> Option<&mut HashMap<String, String>> {
        match self {
            RedisValue::Hash(hash) => Some(hash),
            _ => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RedisValue::String(s) => s.len(),
            RedisValue::List(list) => list.len(),
            RedisValue::Set(set) => set.len(),
            RedisValue::Hash(hash) => hash.len(),
        }
    }
}

/// entry wrapper w expiration support
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub value: RedisValue,
    pub expires_at: Option<SystemTime>,
}

impl Entry {
    pub fn new(value: RedisValue, expires_at: Option<SystemTime>) -> Self {
        Self { value, expires_at }
    }

    pub fn string(value: String, expires_at: Option<SystemTime>) -> Self {
        Self::new(RedisValue::String(value), expires_at)
    }

    pub fn list(expires_at: Option<SystemTime>) -> Self {
        Self::new(RedisValue::List(VecDeque::new()), expires_at)
    }

    pub fn set(expires_at: Option<SystemTime>) -> Self {
        Self::new(RedisValue::Set(HashSet::new()), expires_at)
    }

    pub fn hash(expires_at: Option<SystemTime>) -> Self {
        Self::new(RedisValue::Hash(HashMap::new()), expires_at)
    }

    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at {
            SystemTime::now() > exp
        } else {
            false
        }
    }
} 