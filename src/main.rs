use polars::prelude::*;
use serde::Deserialize;
use futures::future;
use tokio::sync::Mutex;
use dotenv::dotenv;
use std::{
  env, 
  error::Error,
  io::Cursor,
};


#[derive(Deserialize, Debug)]
struct FileInfo {
  download_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  dotenv().ok();
  let folder = env::var("FOLDER").unwrap();
  let github_repo = format!("https://api.github.com/repos/{}", folder);

  let resp = reqwest::Client::new()
    .get(&github_repo)
    .header("User-Agent", "github")
    .send()
    .await?
    .json::<Vec<FileInfo>>()
    .await?;

  let csv_links: Vec<String> = resp
    .into_iter()
    .map(|file: FileInfo| file.download_url)
    .collect();

    let csv_data: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

      let tasks: Vec<_> = csv_links.into_iter().map(|link| {
          let csv_data = Arc::clone(&csv_data);
          tokio::spawn(async move {
              match reqwest::get(&link).await {
                  Ok(response) => {
                      match response.text().await {
                          Ok(text) => {
                              let data = &text[500..];
                              let cleaned_data: String = data
                              .lines()
                              .skip(1)
                              .collect::<Vec<&str>>()
                              .join("\n");
                              println!("{}\n", cleaned_data.trim());
  
                              let mut csv_data = csv_data.lock().await; // Acquire lock
                              csv_data.push(data.trim().to_string()); // Push trimmed data into shared vector
  
                              let data_bytes = text.as_bytes();
                              let cursor = Cursor::new(data_bytes);
  
                              match CsvReader::new(cursor).finish() {
                                  Ok(df) => {
                                      // println!("{:?}", df); // Placeholder for handling parsed DataFrame
                                  }
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
  
      future::join_all(tasks).await;
  
      // let csv_data = csv_data.lock().await;
      // println!("Collected CSV data: {:?}", *csv_data);

  Ok(())
}