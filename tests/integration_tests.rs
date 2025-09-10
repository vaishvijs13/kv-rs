use kvstore::{Store, Response, RedisValue};
use std::time::Duration;

#[tokio::test]
async fn test_basic_string_operations() {
    let store = Store::new(None);

    let result = store.set("key1".to_string(), "value1".to_string(), None);
    assert!(matches!(result, Response::SimpleString(_)));

    let result = store.get("key1");
    if let Response::BulkString(Some(value)) = result {
        assert_eq!(value, "value1");
    } else {
        panic!("Expected BulkString with value");
    }

    let result = store.get("nonexistent");
    assert!(matches!(result, Response::Nil));
}

#[tokio::test]
async fn test_ttl_functionality() {
    let store = Store::new(None);

    // set with TTL
    store.set("temp_key".to_string(), "temp_value".to_string(), Some(1));
    
    // check TTL exists
    let result = store.ttl("temp_key");
    if let Response::Integer(ttl) = result {
        assert!(ttl >= 0 && ttl <= 1);
    } else {
        panic!("Expected integer TTL");
    }

    // wait for expiry
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    let result = store.get("temp_key");
    assert!(matches!(result, Response::Nil));
}

#[tokio::test]
async fn test_increment_operations() {
    let store = Store::new(None);

    let result = store.incr("counter");
    assert_eq!(result.to_string(), "1");

    let result = store.incr("counter");
    assert_eq!(result.to_string(), "2");

    store.set("text".to_string(), "not_a_number".to_string(), None);
    let result = store.incr("text");
    assert!(result.to_string().contains("ERR"));
}

#[tokio::test] 
async fn test_list_operations() {
    let store = Store::new(None);

    let result = store.lpush("mylist", vec!["item1".to_string(), "item2".to_string()]);
    assert_eq!(result.to_string(), "2");

    let result = store.llen("mylist");
    assert_eq!(result.to_string(), "2");

    let result = store.lpop("mylist");
    if let Response::BulkString(Some(value)) = result {
        assert_eq!(value, "item1"); // first item pushed becomes head after reversing
    } else {
        panic!("Expected BulkString with value");
    }

    let result = store.llen("mylist");
    assert_eq!(result.to_string(), "1");

    store.lpop("mylist");
    let result = store.lpop("mylist");
    assert!(matches!(result, Response::Nil));
}

#[tokio::test]
async fn test_set_operations() {
    let store = Store::new(None);

    let result = store.sadd("myset", vec!["member1".to_string(), "member2".to_string(), "member1".to_string()]);
    assert_eq!(result.to_string(), "2"); // member1 added only once

    let result = store.scard("myset");
    assert_eq!(result.to_string(), "2");

    let result = store.srem("myset", vec!["member1".to_string()]);
    assert_eq!(result.to_string(), "1");

    let result = store.scard("myset");
    assert_eq!(result.to_string(), "1");

    let result = store.srem("myset", vec!["nonexistent".to_string()]);
    assert_eq!(result.to_string(), "0");
}

#[tokio::test]
async fn test_type_safety() {
    let store = Store::new(None);

    store.set("string_key".to_string(), "string_value".to_string(), None);

    let result = store.lpush("string_key", vec!["item".to_string()]);
    assert!(result.to_string().contains("WRONGTYPE"));

    let result = store.lpop("string_key");
    assert!(result.to_string().contains("WRONGTYPE"));

    let result = store.sadd("string_key", vec!["member".to_string()]);
    assert!(result.to_string().contains("WRONGTYPE"));
}

#[tokio::test]
async fn test_key_expiration_cleanup() {
    let store = Store::new(None);

    // set keys with short TTL
    store.set("key1".to_string(), "value1".to_string(), Some(1));
    store.set("key2".to_string(), "value2".to_string(), None);

    let result = store.exists("key1");
    assert_eq!(result.to_string(), "1");
    let result = store.exists("key2");
    assert_eq!(result.to_string(), "1");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let result = store.exists("key1");
    assert_eq!(result.to_string(), "0");

    // check that the non-expired key still exists
    let result = store.exists("key2");
    assert_eq!(result.to_string(), "1");
}

#[test] 
fn test_protocol_parsing() {
    use kvstore::protocol::handle_command;
    
    let store = Store::new(None);

    let result = handle_command(&store, "PING");
    assert_eq!(result.to_string(), "PONG");

    let result = handle_command(&store, "INVALID");
    assert!(result.to_string().contains("ERR unknown command"));

    // test wrong argss
    let result = handle_command(&store, "GET");
    assert!(result.to_string().contains("wrong number of arguments"));
} 