use polars::prelude::*;
use serde::Deserialize;
use futures::future;
use tokio::sync::Mutex;
use dotenv::dotenv;
use std::{
  env, 
  error::Error, 
  io::Cursor,
  ops::BitAnd
};
use polars::prelude::CsvReader;
use chrono::{NaiveDateTime, NaiveDate};

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

  let timestamp_col = "Timestamp";
  let mut running_df = running_df
    .filter(&running_df.column(&timestamp_col)?.is_not_null())?
    .sort([timestamp_col], Default::default())?;
  // println!("{:?}", running_df);

  // running_df.filter(&running_df["Date"].str().map(|date: Option<&str>| date.));

  let distance5k = running_df
  .filter(&running_df["Distance(km)"].f64().unwrap().gt(5.1))?; // Use `gt` for comparison

  // let indoor = running_df.filter(&activity_column.equal("indoor"))?;
  let indoor = activity_filter(&running_df, "indoor");
  let outdoor = activity_filter(&running_df, "outdoor");


  // println!("{:?}", indoor);
  // println!("{:?}", outdoor);

  let distance_400m = filter_distance(&running_df, 0.38, 0.43)?;
  let distance_1k = filter_distance(&running_df, 1.0, 1.1)?;
  let distance_1_2k = filter_distance(&running_df, 1.2, 1.35)?;
  let distance_2k = filter_distance(&running_df, 2.0, 2.25)?;
  

  let d = sum_distance(&distance_1k)?;
  println!("{}", d);

  // let col = distance_400m.select(["Distance(km)"]);

  let format = "%Y-%m-%d %H:%M:%S";
  let date_str = "2024-01-01 00:00:00";

  let date = NaiveDateTime::parse_from_str(&date_str, format)
    .ok().expect("Invalid date");
  
  let ts = date.and_utc().timestamp();
  println!("{}", ts);
    // .map(|dt| dt.and_utc().timestamp())

  let jan_2025 = filter_month(&running_df, "2025-01")?;
  let sum_dis_jan_2025 = sum_distance(&jan_2025)?;

  let group = running_df.group_by(["Activity"])?.select(["Distance(km)"]).sum();

  let only_date_df = &running_df
    .apply("Date", only_date_column)?
    .sort([timestamp_col], Default::default())?;

  let month_sum = &only_date_df
    .group_by(["Date"])?
    .select(["Distance(km)"])
    .sum()?
    .sort(["Date"], Default::default())?;

  let struct_array = dataframe_to_struct_vec(&month_sum)?;
  // for entry in struct_array {
  //   println!("Date: {}, Distance: {}", entry.date, entry.distance);
  // }
   // Pretty format with indentation

  let only_2024 = contain_year(month_sum, "2567")?;
  let mut full_2024 = fill_missing_months(&only_2024)?;

  let full_2024 = full_2024.rename("Distance(km)_sum", "Distance(km)".into())?;

  // println!("{:#?}", full_2024);

  let unique_month = full_2024.column("Date")?.unique()?.sort(Default::default())?;
  let unique_count = full_2024.column("Date")?.n_unique()?;

  let sum_distance_2024 = sum_distance(&full_2024)?;
  // println!("{}", sum_distance_2024);

  let month_name = number_to_month(01).expect("No month"); // Converts 1 to "January"
  // println!("{:?}", month_name);

  let full_month_2024 = full_2024.apply("Date", convert_date_month)?;

  let mut df = df!(
    "Value" => &[10, 25, 45, 55, 75, 90, 120, 150, 175, 200]
  )?;

  let value_column = df.column("Value")?.i32()?;

  let range_column: Vec<String> = value_column
    .into_iter()
    .map(|val| match val {
        Some(v) if v <= 50 => "0-50".to_string(),
        Some(v) if v <= 100 => "51-100".to_string(),
        Some(v) if v <= 150 => "101-150".to_string(),
        _ => "151-200".to_string(),
    })
    .collect();

  df.with_column(Series::new("Range".into(), range_column))?;

  let grouped_df = df
    .group_by(["Range"])?
    .count()?
    .sort(["Range"], Default::default());

  println!("{:?}", grouped_df);

  

  // println!("{:?}", full_month_2024);
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

fn only_date_column(date_col: &Column) -> Column {
  date_col
    .str()
    .unwrap()
    .into_iter()
    .map(|d| d.and_then(|dd| Some(&dd[..7])))
    // .map(|d| {
    //   match d {
    //     Some(dd) => Some(&dd[..7]),
    //     _ => Some("")
    //   }
    // })
    .collect::<StringChunked>()
    .into_column()
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
 
fn activity_filter(df: &DataFrame, activity: &str) -> PolarsResult<DataFrame> {
  let activity_column = df.column("Activity")?.str()?;
  let mask = activity_column.equal(activity);
  df.filter(&mask)
}

fn contain_year(df: &DataFrame, year: &str) -> PolarsResult<DataFrame> {
  let activity_column = df.column("Date")?.str()?;
  let mask = activity_column
    .into_iter()    
    .map(|opt_val| opt_val.map(|val| val.starts_with(year)))
    .collect::<BooleanChunked>();

  df.filter(&mask)

}

fn filter_distance(df: &DataFrame, min: f64, max: f64) -> PolarsResult<DataFrame> {
  let distance_column = df.column("Distance(km)")?.f64()?;
  let mask = distance_column
    .gt(min)
    .bitand(distance_column.lt(max));
  df.filter(&mask)
}

fn sum_distance(df: &DataFrame) -> PolarsResult<f64> {
  let distance_column = df.column("Distance(km)")?.f64()?;
  Ok(distance_column.sum().unwrap_or(0.0))
}

fn convert_date_timestamp(date: &str) ->i64 {
  let format = "%Y-%m-%d %H:%M:%S";
  let date = NaiveDateTime::parse_from_str(&date, format)
    .ok()
    .expect("Invalid date");

  date.and_utc().timestamp()
}

fn filter_month(df: &DataFrame, year_month: &str) -> PolarsResult<DataFrame> {
  let date_part: Vec<i64> = year_month
    .split('-')
    .filter_map(|part| part.parse::<i64>().ok()) 
    .collect();
  let (year, month) = (date_part[0], date_part[1]);

  let end_month = if month == 12 {
    format!("{}-{}", year + 1, 1)
  } else {
    format!("{}-{}", year, month + 1)
  };

  let start_date = format!("{}-01 00:00:00", year_month);
  let end_date = format!("{}-01 00:00:00", end_month);

  println!("{} {}", start_date,  end_date);

  let start_timestamp = convert_date_timestamp(&start_date);
  let end_timestamp = convert_date_timestamp(&end_date);

  let distance_column = df.column("Timestamp")?.i64()?;
  let mask = distance_column
    .gt(start_timestamp)
    .bitand(distance_column.lt(end_timestamp));

  df.filter(&mask)
}

#[allow(dead_code)]
#[derive(Debug)]
struct MonthlyDistance {
  date: String,
  distance: f64,
}

fn dataframe_to_struct_vec(df: &DataFrame) -> PolarsResult<Vec<MonthlyDistance>> {
  let dates = df.column("Date")?.str()?;
  let distances = df.column("Distance(km)_sum")?.f64()?;

  let struct_vec: Vec<MonthlyDistance> = dates
    .into_no_null_iter()
    .zip(distances.into_no_null_iter())
    .map(|(date, distance)| MonthlyDistance {
      date: date.to_string(),
      distance,
    })
    .collect();

  Ok(struct_vec)
}

fn fill_missing_months(df: &DataFrame) -> PolarsResult<DataFrame> {
  let months: Vec<String> = (1..=12)
    .map(|m| format!("2567-{:02}", m)) 
    .collect();
  
  let full_months_df = df!("Date" => &months)?;

  let result = full_months_df
    .left_join(df, ["Date"], ["Date"])?
    .fill_null(FillNullStrategy::Zero)?;

  Ok(result)
}

fn number_to_month(num: u32) -> Option<String> {
  NaiveDate::from_ymd_opt(2000, num, 1)
    .map(|d| d.format("%B").to_string())
}

fn convert_date_month(str_val: &Column) -> Column {
  str_val.str()
    .unwrap() 
    .into_iter()
    .map(|d| {
      d.and_then(|dd| {
        let month_str = &dd[5..7];
        month_str.parse::<u32>().ok().and_then(|month_num| {
          number_to_month(month_num)
        })
      })
    })
    .collect::<StringChunked>()
    .into_column()
}

