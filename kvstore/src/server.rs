use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use crate::{store::Store, protocol::handle_command, aof::Aof};

pub async fn run(addr: &str, aof_path: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let aof = Aof::new(aof_path).await.ok();
    let store = Store::new(aof.clone());

    // replay AOF
    if let Ok(entries) = crate::aof::Aof::replay(aof_path) {
        store.load_from_aof(entries);
    }

    tokio::spawn(store.clone().start_sweeper(2));

    println!("Listening on {addr}");
    loop {
        let (socket, peer) = listener.accept().await?;
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, store).await {
                eprintln!("client {peer:?} error: {e:?}");
            }
        });
    }
}

async fn handle_client(stream: TcpStream, store: Store) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }

        let resp = handle_command(&store, &line);
        if resp == "BYE" {
            writer.write_all(b"Bye!!!\n").await?;
            break;
        } else {
            writer.write_all(format!("{resp}\n").as_bytes()).await?;
        }
    }
    Ok(())
}
