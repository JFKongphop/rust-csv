use polars::prelude::*;
use serde::Deserialize;
use futures::future;
use tokio::sync::Mutex;
use dotenv::dotenv;
use std::{
    env,
    error::Error,
    io::Cursor,
    sync::Arc,
};

#[derive(Deserialize, Debug)]
struct FileInfo {
    download_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let folder = env::var("FOLDER")?;
    let github_repo = format!("https://api.github.com/repos/{}", folder);

    // Fetching the list of file information (CSV URLs) from GitHub
    let resp = reqwest::Client::new()
        .get(&github_repo)
        .header("User-Agent", "github")
        .send()
        .await?
        .json::<Vec<FileInfo>>()
        .await?;

    // Extract CSV links
    let csv_links: Vec<String> = resp
        .into_iter()
        .map(|file: FileInfo| file.download_url)
        .collect();

    // Shared data frame storage protected by a Mutex
    let dataframes = Arc::new(Mutex::new(Vec::new()));

    // Create tasks to download and process CSV files concurrently
    let tasks: Vec<_> = csv_links.into_iter().map(|link| {
        let dataframes = Arc::clone(&dataframes); // Clone the Arc to share between tasks
        tokio::spawn(async move {
            match reqwest::get(&link).await {
                Ok(response) => {
                    match response.text().await {
                        Ok(text) => {
                            let data_bytes = text.as_bytes();
                            let cursor = Cursor::new(data_bytes);

                            match CsvReader::new(cursor).finish() {
                                Ok(df) => {
                                    // Lock the dataframes vector and push the new dataframe
                                    let mut dataframes = dataframes.lock().await;
                                    dataframes.push(df);
                                }
                                Err(e) => {
                                    eprintln!("Error reading CSV from {}: {}", link, e);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error fetching CSV text from {}: {}", link, e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error downloading from {}: {}", link, e);
                }
            }
        })
    }).collect();

    // Wait for all tasks to finish
    future::join_all(tasks).await;

    // Lock the dataframes to access the final result
    let dataframes = dataframes.lock().await;
    println!("Merged DataFrames: {:?}", dataframes);

    Ok(())
}
