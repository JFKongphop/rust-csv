use polars::prelude::*;
use serde::Deserialize;
use futures::future;
use tokio::sync::Mutex;
use dotenv::dotenv;
use std::{
  env, error::Error, fs::File, io::Cursor
};
// use polars_lazy::{dsl::functions::concat, frame::LazyFrame, prelude::UnionArgs};
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
  let mut header: &str = "Date,Energy (kcal),Activity,Distance(km),Duration(min),Pace(min),Heart rate: Average(min),Heart rate: Maximum(min)\n";

  let tasks: Vec<_> = csv_links.into_iter().map(|link| {
    let csv_data = Arc::clone(&csv_data);
    tokio::spawn(async move {
      match reqwest::get(&link).await {
        Ok(response) => {
          match response.text().await {
            Ok(text) => {
              // let data = &text[500..];
              // let cleaned_data: String = data
              //   .lines()
              //   .skip(1)
              //   .collect::<Vec<&str>>()
              //   .join("\n");
              // // println!("{}\n", cleaned_data.trim());

              let mut csv_data = csv_data.lock().await;
              // csv_data.push(text.trim().to_string());

              let data_bytes = text.as_bytes();
              let cursor = Cursor::new(data_bytes);

              match CsvReader::new(cursor).finish() {//.finish() {
                Ok(_df) => {
                  // println!("{:?}\n", text.replace(header, ""));
                  csv_data.push(text.replace(header, ""));
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

  let joined_csv_data = {
    // let csv_data = csv_data.lock().await;
    let csv_data = csv_data.lock().await;
    csv_data.join("\n") // Join with newline as the delimiter
  };

  let data_bytes = joined_csv_data.as_bytes();
  let cursor = Cursor::new(data_bytes);

  match CsvReader::new(cursor).finish() {
    Ok(df) => {
      println!("{:?}", df);
      
    }
    Err(e) => eprintln!("Error reading CSV from: {}", e),
  }


  // Configure CSV read options
  // let csv_options = CsvReadOptions {
  //     has_header: true, // If the CSV has a header
  //     // Other options like separator, dtype, etc.
  //     ..Default::default()
  // };

  // let link = "https://your-csv-file-url.csv";

  // // Fetch the CSV data from the link
  // let resp = reqwest::get(link)
  //     .await?
  //     .text().await?;

  // // Convert the response text to bytes and create a Cursor
  // let data_bytes = resp.as_bytes();
  // let cursor = Cursor::new(data_bytes);

  // CsvReadOptions::default()
  // .with_has_header(true)
  // .try_into_reader_with_file_path(Some("iris.csv".into()))?
  // .finish();



  Ok(())
}
