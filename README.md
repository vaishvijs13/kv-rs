# redis implementation in rust


### Redis Commands
- **String Operations**: `GET`, `SET`, `DEL`, `EXISTS`, `TTL`, `INCR`
- **List Operations**: `LPUSH`, `LPOP`, `LLEN`
- **Set Operations**: `SADD`, `SREM`, `SCARD`
- **Utility**: `PING`, `KEYS`, `QUIT`

### Other Features
- **TTL Support**: Automatic key expiration with background cleanup
- **Persistence**: Append-Only File (AOF) for data durability
- **Concurrency**: Async/await with Tokio runtime
- **Type Safety**: Strong typing with custom error handling
- **Memory Management**: Efficient concurrent data structures