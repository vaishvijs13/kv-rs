use crate::{store::Store, error::{RedisError, Response}};

pub fn handle_command(store: &Store, input: &str) -> Response {
    let line = input.trim();
    if line.is_empty() {
        return RedisError::InvalidCommand("empty command".to_string()).into();
    }
    
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return RedisError::InvalidCommand("empty command".to_string()).into();
    }
    
    let cmd = parts[0].to_uppercase();

    match cmd.as_str() {
        "PING" => Response::SimpleString("PONG".to_string()),
        "QUIT" => Response::SimpleString("BYE".to_string()),

        // string ops
        "SET" => {
            if parts.len() < 3 {
                return RedisError::WrongArguments { 
                    command: "SET".to_string(), 
                    expected: "at least 3".to_string(), 
                    got: parts.len() 
                }.into();
            }
            let key = parts[1].to_string();

            if parts.len() >= 5 && parts[parts.len()-2].eq_ignore_ascii_case("EX") {
                let ttl = parts.last().and_then(|s| s.parse::<u64>().ok());
                if ttl.is_none() { 
                    return RedisError::InvalidType("invalid EX ttl".to_string()).into(); 
                }
                // value is between parts[2..len-2]
                let value = parts[2..parts.len()-2].join(" ");
                if value.is_empty() {
                    return RedisError::InvalidType("empty value".to_string()).into();
                }
                store.set(key, value, ttl)
            } else {
                let value = parts[2..].join(" ");
                if value.is_empty() { 
                    return RedisError::InvalidType("empty value".to_string()).into(); 
                }
                store.set(key, value, None)
            }
        }

        "GET" => {
            if parts.len() != 2 { 
                return RedisError::WrongArguments { 
                    command: "GET".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into(); 
            }
            store.get(parts[1])
        }

        "DEL" => {
            if parts.len() != 2 { 
                return RedisError::WrongArguments { 
                    command: "DEL".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into(); 
            }
            store.del(parts[1])
        }

        "EXISTS" => {
            if parts.len() != 2 { 
                return RedisError::WrongArguments { 
                    command: "EXISTS".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into(); 
            }
            store.exists(parts[1])
        }

        "TTL" => {
            if parts.len() != 2 { 
                return RedisError::WrongArguments { 
                    command: "TTL".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into(); 
            }
            store.ttl(parts[1])
        }

        "KEYS" => {
            if parts.len() != 2 { 
                return RedisError::WrongArguments { 
                    command: "KEYS".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into(); 
            }
            store.keys_with_prefix(parts[1])
        }

        "INCR" => {
            if parts.len() != 2 { 
                return RedisError::WrongArguments { 
                    command: "INCR".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into(); 
            }
            store.incr(parts[1])
        }

        // list ops
        "LPUSH" => {
            if parts.len() < 3 {
                return RedisError::WrongArguments { 
                    command: "LPUSH".to_string(), 
                    expected: "at least 2".to_string(), 
                    got: parts.len() - 1 
                }.into();
            }
            let key = parts[1];
            let values: Vec<String> = parts[2..].iter().map(|s| s.to_string()).collect();
            store.lpush(key, values)
        }

        "LPOP" => {
            if parts.len() != 2 {
                return RedisError::WrongArguments { 
                    command: "LPOP".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into();
            }
            store.lpop(parts[1])
        }

        "LLEN" => {
            if parts.len() != 2 {
                return RedisError::WrongArguments { 
                    command: "LLEN".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into();
            }
            store.llen(parts[1])
        }

        // set ops
        "SADD" => {
            if parts.len() < 3 {
                return RedisError::WrongArguments { 
                    command: "SADD".to_string(), 
                    expected: "at least 2".to_string(), 
                    got: parts.len() - 1 
                }.into();
            }
            let key = parts[1];
            let members: Vec<String> = parts[2..].iter().map(|s| s.to_string()).collect();
            store.sadd(key, members)
        }

        "SREM" => {
            if parts.len() < 3 {
                return RedisError::WrongArguments { 
                    command: "SREM".to_string(), 
                    expected: "at least 2".to_string(), 
                    got: parts.len() - 1 
                }.into();
            }
            let key = parts[1];
            let members: Vec<String> = parts[2..].iter().map(|s| s.to_string()).collect();
            store.srem(key, members)
        }

        "SCARD" => {
            if parts.len() != 2 {
                return RedisError::WrongArguments { 
                    command: "SCARD".to_string(), 
                    expected: "1".to_string(), 
                    got: parts.len() - 1 
                }.into();
            }
            store.scard(parts[1])
        }

        _ => RedisError::InvalidCommand(cmd).into(),
    }
}
