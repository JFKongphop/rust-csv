use polars::prelude::*;
use serde::Deserialize;
use futures::future;
use std::{
  error::Error,
  io::Cursor,
};


#[derive(Deserialize, Debug)]
struct FileInfo {
  download_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let url = "https://api.github.com/repos/JFKongphop/running-analysis/contents/running";

  let resp = reqwest::Client::new()
    .get(url)
    .header("User-Agent", "github")
    .send()
    .await?
    .json::<Vec<FileInfo>>()
    .await?;

  let csv_links: Vec<String> = resp
    .into_iter()
    .map(|file: FileInfo| file.download_url)
    .collect();

  println!("{:#?}", csv_links);

  let tasks: Vec<_> = csv_links.into_iter().map(|link| {
    tokio::spawn(async move {
      match reqwest::get(&link).await {
        Ok(response) => {
          match response.text().await {
            Ok(text) => {
              let data_bytes = text.as_bytes();
              let cursor = Cursor::new(data_bytes);

              match CsvReader::new(cursor).finish() {
                Ok(df) => println!("{:?}", df),
                Err(e) => eprintln!("Error reading CSV from {}: {}", link, e),
              }
            }
            Err(e) => eprintln!("Error fetching CSV from {}: {}", link, e),
          }
        }
        Err(e) => eprintln!("Error downloading from {}: {}", link, e),
      }
    })
  }).collect();

  let _ = future::join_all(tasks).await;

  Ok(())
}