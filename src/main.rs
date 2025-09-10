mod server;
mod store;
mod protocol;
mod aof;
mod error;
mod types;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("KV_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    let aof_path = std::env::var("KV_AOF").unwrap_or_else(|_| "kvstore.aof".to_string());

    println!("KVStore starting on {addr} (AOF: {aof_path})");

    // start server
    let srv = server::run(&addr, &aof_path);

    tokio::select! {
        res = srv => {
            if let Err(e) = res {
                eprintln!("Server error: {e:?}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down");
        }
    }

    Ok(())
}