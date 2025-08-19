use crate::store::Store;

pub fn handle_command(store: &Store, input: &str) -> String {
    let line = input.trim();
    if line.is_empty() {
        return "ERR empty command".into();
    }
    let mut parts: Vec<&str> = line.split_whitespace().collect();
    let cmd = parts[0].to_uppercase();

    match cmd.as_str() {
        "PING" => "PONG".into(),
        "QUIT" => "BYE".into(),

        "SET" => {
            if parts.len() < 3 {
                return "ERR wrong number of arguments for 'SET'".into();
            }
            let key = parts[1].to_string();

            let mut ttl: Option<u64> = None;
            if parts.len() >= 5 && parts[parts.len()-2].eq_ignore_ascii_case("EX") {
                ttl = parts.last().and_then(|s| s.parse::<u64>().ok());
                if ttl.is_none() { return "ERR invalid EX ttl".into(); }
                // value is between parts[2..len-2]
                let value = parts[2..parts.len()-2].join(" ");
                if value.is_empty() {
                    return "ERR empty value".into();
                }
                return store.set(key, value, ttl);
            } else {
                let value = parts[2..].join(" ");
                if value.is_empty() { return "ERR empty value".into(); }
                return store.set(key, value, None);
            }
        }

        "GET" => {
            if parts.len() != 2 { return "ERR wrong number of arguments for 'GET'".into(); }
            store.get(parts[1])
        }

        "DEL" => {
            if parts.len() != 2 { return "ERR wrong number of arguments for 'DEL'".into(); }
            store.del(parts[1])
        }

        "EXISTS" => {
            if parts.len() != 2 { return "ERR wrong number of arguments for 'EXISTS'".into(); }
            store.exists(parts[1])
        }

        "TTL" => {
            if parts.len() != 2 { return "ERR wrong number of arguments for 'TTL'".into(); }
            store.ttl(parts[1])
        }

        "KEYS" => {
            if parts.len() != 2 { return "ERR wrong number of arguments for 'KEYS'".into(); }
            let keys = store.keys_with_prefix(parts[1]);
            if keys.is_empty() { "(empty)".into() } else { keys.join(" ") }
        }

        "INCR" => {
            if parts.len() != 2 { return "ERR wrong number of arguments for 'INCR'".into(); }
            match store.incr(parts[1]) {
                Ok(v) => v,
                Err(e) => e,
            }
        }

        _ => "ERR unknown command".into(),
    }
}
