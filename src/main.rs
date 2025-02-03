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
use chrono::NaiveDateTime;

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

  let mut running_df: DataFrame = CsvReader::new(cursor)
    .finish()
    .expect("CSV reading should not fail");

  // let mut running_df = running_df.drop("Date").expect("Invalid Date");
  let _ = running_df.apply("Activity", activity_to_type);

  let format = "%Y-%m-%d %H:%M:%S"; // Define the format of the input string
  let full_date = "2567-03-02 17:55:38 - 18:25:47";
  let only_start = &full_date[..19];
  let (db_year, date_time) = only_start.split_at(4);
  let year: i32 = db_year.parse::<i32>()? - 543;
  
  let date_str = format!("{}{}", year, date_time);
  println!("{}", date_str);

  // Parse the string into a NaiveDateTime
  let naive_datetime = NaiveDateTime::parse_from_str(&date_str, format)?;

  // Convert to timestamp correctly
  let timestamp = naive_datetime.and_utc().timestamp();
  
  println!("Unix timestamp: {}", timestamp);
  // let date_lenght = "2567-03-02 17:55:38".len();
  // let slice = &text[..19]; // "Hello"
  // println!("{}", slice);

  let timestamp_col = running_df
    .column("Date")?
    .str()?
    .into_iter()
    .map(|date_opt: Option<&str>| 
      date_opt.and_then(
        |full_date| date_to_timestamp(full_date)
      )
    )
    .collect::<Int64Chunked>()
    .into_series()
    .with_name("Timestamp".into());

  let _ = running_df.with_column(timestamp_col);

  let _ = running_df.sort(["Timestamp"], Default::default());

  

  // VALID
  // let s0 = Series::new("Date".into(), &[
  //   "2567-03-02 17:55:38 - 18:25:47",
  //   "2567-04-10 14:30:12 - 15:00:45",
  //   "2567-05-20 09:15:05 - 09:45:30",
  // ]);

  // let mut df = DataFrame::new(vec![s0.into()])?;

  // let date_col = df
  // .column("Date")
  // .unwrap()
  // .str() // Convert to string chunked series
  // .unwrap()
  // .into_iter()
  // .map(|opt_str| opt_str.map(|s| s.split_whitespace().next().unwrap_or("")))
  // .collect::<StringChunked>().into_column();
  // df.with_column(date_col).unwrap(); // A

  // println!("{}", df);

//   let s0 = Series::new("Date".into(), &[
//     "2567-03-02 17:55:38 - 18:25:47",
//     "2567-04-10 14:30:12 - 15:00:45",
//     "2567-05-20 09:15:05 - 09:45:30",
// ]);

// // Create a DataFrame with the "Date" column
// let mut df = DataFrame::new(vec![s0.into()])?;

// let col = df
//     .column("Date")
//     .unwrap()
//     .str()
//     .unwrap()
//     .into_iter()
//     .map(|opt_str| opt_str.map(|s| s.split_whitespace().next().unwrap_or("")))
//     .collect::<StringChunked>(); // Collect into StringChunked

// // Convert the ChunkedArray into Series and rename it, binding to a variable
// let date_col = col.into_series();// .rename("OnlyDate".into());

// // Add the new column to the DataFrame


// df.with_column(date_col)?; // Now this works because date_col is no longer tempora

// df.rename("", "helloworld".into());

// // Print the DataFrame
  println!("{}", running_df);

  





  Ok(())
}

fn activity_to_type(str_val: &Column) -> Column {
  str_val.str()
    .unwrap() 
    .into_iter()
    .map(|opt_name: Option<&str>| {
        match opt_name {
          Some(val) if val.contains("indoor") => Some("indoor"),
          _ => Some("outdoor"),
        }
      }
    )
    .collect::<StringChunked>().into_column()
}


fn date_to_timestamp(full_date: &str) -> Option<i64> {
  let format = "%Y-%m-%d %H:%M:%S";
  let only_start = &full_date[..19];
  let (db_year, date_time) = only_start.split_at(4);
  let year = db_year.parse::<i32>().ok()? - 543;
  let date_str = format!("{}{}", year, date_time);

  NaiveDateTime::parse_from_str(&date_str, format)
    .ok()
    .map(|dt| dt.and_utc().timestamp())
}