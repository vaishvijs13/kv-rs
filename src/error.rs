use std::fmt;

#[derive(Debug, Clone)]
pub enum RedisError {
    /// invalid command syntax
    InvalidCommand(String),
    /// wrong number of arguments
    WrongArguments { command: String, expected: String, got: usize },
    /// invalid data type for operation
    InvalidType(String),
    /// key not found
    KeyNotFound(String),
    /// value cannot be parsed as integer
    NotInteger(String),
    /// internal server error
    Internal(String),
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisError::InvalidCommand(cmd) => write!(f, "ERR unknown command '{}'", cmd),
            RedisError::WrongArguments { command, expected, got } => {
                write!(f, "ERR wrong number of arguments for '{}' command. Expected {}, got {}", command, expected, got)
            },
            RedisError::InvalidType(msg) => write!(f, "ERR {}", msg),
            RedisError::KeyNotFound(key) => write!(f, "ERR key '{}' not found", key),
            RedisError::NotInteger(val) => write!(f, "ERR value '{}' is not an integer or out of range", val),
            RedisError::Internal(msg) => write!(f, "ERR internal error: {}", msg),
        }
    }
}

impl std::error::Error for RedisError {}

pub type RedisResult<T> = Result<T, RedisError>;

#[derive(Debug, Clone)]
pub enum Response {
    SimpleString(String),
    Error(RedisError),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<Response>),
    Nil,
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Response::SimpleString(s) => write!(f, "{}", s),
            Response::Error(e) => write!(f, "{}", e),
            Response::Integer(i) => write!(f, "{}", i),
            Response::BulkString(Some(s)) => write!(f, "{}", s),
            Response::BulkString(None) | Response::Nil => write!(f, "(nil)"),
            Response::Array(arr) => {
                if arr.is_empty() {
                    write!(f, "(empty)")
                } else {
                    let strs: Vec<String> = arr.iter().map(|r| r.to_string()).collect();
                    write!(f, "{}", strs.join(" "))
                }
            }
        }
    }
}

impl From<RedisError> for Response {
    fn from(error: RedisError) -> Self {
        Response::Error(error)
    }
}

impl From<String> for Response {
    fn from(s: String) -> Self {
        Response::SimpleString(s)
    }
}

impl From<&str> for Response {
    fn from(s: &str) -> Self {
        Response::SimpleString(s.to_string())
    }
}

impl From<i64> for Response {
    fn from(i: i64) -> Self {
        Response::Integer(i)
    }
} 