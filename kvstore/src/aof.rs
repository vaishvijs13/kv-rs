use serde::{Deserialize, Serialize};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::mpsc};
use std::{fs, io::{BufRead, BufReader}, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub op: String,
    pub key: String,
    pub value: Option<String>,
    pub expires_at_ms: Option<i64>,
}

#[derive(Clone)]
pub struct Aof {
    tx: mpsc::UnboundedSender<LogEntry>,
}

impl Aof {
    pub async fn new(path: &str) -> anyhow::Result<Self> {
        // check that the file exists
        if !Path::new(path).exists() {
            tokio::fs::File::create(path).await?;
        }
        let (tx, mut rx) = mpsc::unbounded_channel::<LogEntry>();
        let path = path.to_string();

        tokio::spawn(async move {
            let file_res = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await;

            let mut file = match file_res {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("AOF open error: {e}");
                    return;
                }
            };

            while let Some(entry) = rx.recv().await {
                if let Ok(line) = serde_json::to_string(&entry) {
                    if let Err(e) = file.write_all(line.as_bytes()).await {
                        eprintln!("AOF write error: {e}");
                        break;
                    }
                    if let Err(e) = file.write_all(b"\n").await {
                        eprintln!("AOF write error: {e}");
                        break;
                    }
                    // fsync could be added; omitted for perf
                }
            }
        });

        Ok(Self { tx })
    }

    pub fn log(&self, entry: LogEntry) {
        // fire n forget
        let _ = self.tx.send(entry);
    }

    pub fn replay(path: &str) -> anyhow::Result<Vec<LogEntry>> {
        if !Path::new(path).exists() {
            return Ok(vec![]);
        }
        let file = fs::File::open(path)?;
        let reader = BufReader::new(file);

        let mut entries = Vec::new();
        for line_res in reader.lines() {
            let line = line_res?;
            if line.trim().is_empty() { continue; }
            match serde_json::from_str::<LogEntry>(&line) {
                Ok(e) => entries.push(e),
                Err(e) => eprintln!("AOF replay parse error: {e} (line: {line})"),
            }
        }
        Ok(entries)
    }
}