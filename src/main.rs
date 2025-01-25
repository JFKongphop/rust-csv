// use polars::prelude::*;
// use std::collections::HashMap;
// use std::io::Cursor;
use serde::Deserialize;
use std::error::Error;

// Define the FileInfo struct to match the JSON structure
#[derive(Deserialize, Debug)]
struct FileInfo {
  // name: String,
  // path: String,
  // sha: String,
  // size: u64,
  // url: String,
  // html_url: String,
  // git_url: String,
  download_url: String,
  // #[serde(rename = "type")]
  // file_type: String,
  // _links: Links,
}

// #[derive(Deserialize, Debug)]
// struct Links {
//   #[serde(rename = "self")]
//   self_url: String,
//   git: String,
//   html: String,
// }

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn Error>> {
//   let url = "https://api.github.com/repos/JFKongphop/running-analysis/contents/running";
//   let resp = reqwest::Client::new().get(url).header("User-Agent", "reqwest").send().await?.text().await?;
  


//   // let url = "https://raw.githubusercontent.com/JFKongphop/running-analysis/main/running/01-oct-2023.csv";

//   // let resp = reqwest::get(url)
//   //   .await?.text().await?;
//   //   // .json::<HashMap<String, String>>()
//   //   // .await?;
//   // println!("{:#?}", resp);

//   // let data_bytes = resp.as_bytes();

//   // let cursor = Cursor::new(data_bytes);
  

//   // // Use Polars' CsvReader to read the data without explicitly inferring the schema
//   // let df = CsvReader::new(cursor).
//   //     finish()?; // Automatically infer schema during reading

//   // // Print the DataFrame
//   // println!("{:?}", df);

//   // Ok(())
// }


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // GitHub API URL
    let url = "https://api.github.com/repos/JFKongphop/running-analysis/contents/running";

    // Make the HTTP GET request using reqwest
    let resp = reqwest::Client::new()
        .get(url)
        .header("User-Agent", "reqwest") // GitHub API requires a User-Agent header
        .send()
        .await?
        .json::<Vec<FileInfo>>() // Deserialize into Vec<FileInfo>
        .await?;

    // Print the deserialized response
    println!("{:#?}", resp);

    // Example: Access individual file info
    for file in &resp {
        println!("File: {}", file.download_url);
    }

    Ok(())
}