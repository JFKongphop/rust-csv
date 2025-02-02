use polars::prelude::*;
use serde::Deserialize;
use futures::future;
use tokio::sync::Mutex;
use dotenv::dotenv;
use std::{
  env, 
  error::Error, 
  io::Cursor
};
use polars::prelude::CsvReader;

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
  let header = String::from("Date,Energy (kcal),Activity,Distance(km),Duration(min),Pace(min),Heart rate: Average(min),Heart rate: Maximum(min)\n");

  let tasks: Vec<_> = csv_links.into_iter().map(|link| {
    let csv_data = Arc::clone(&csv_data);
    let header = header.clone();

    tokio::spawn(async move {
      match reqwest::get(&link).await {
        Ok(response) => {
          match response.text().await {
            Ok(text) => {
              let mut csv_data = csv_data.lock().await;
              let data_bytes = text.as_bytes();
              let cursor = Cursor::new(data_bytes);

              match CsvReader::new(cursor).finish() {
                Ok(_df) => {
                  csv_data.push(text.replace(header.as_str(), ""));
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

  let joined_csv_data = {
    let csv_data = csv_data.lock().await;
    csv_data.join("\n")
  };

  let new_data = format!("{}{}", header, joined_csv_data);
  let data_bytes = new_data.as_bytes();
  let cursor = Cursor::new(data_bytes);

  let running_df: DataFrame = CsvReader::new(cursor)
    .finish()
    .expect("CSV reading should not fail");

  let running_df = running_df.drop("Date").expect("Invalid Date");
  println!("{:?}", running_df);


  

  Ok(())
}
