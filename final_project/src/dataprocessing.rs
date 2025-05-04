use std::error::Error;
use csv;
use std::fmt;
use std::path::PathBuf;

// The purpose of this module is to just handle the reading of the data into the csv file.

//The process of reading this csv is the same from the previous, so i used that as a framework

// Column struct represents each column in dataframe
#[derive(Debug, Clone)]
pub struct Column {
    pub label: String, 
    pub data: Vec<ColumnVal>,

}
// stores option of column, representing the entire dataframe
#[derive(Debug, Clone)]
pub struct DataFrame {
    pub columns: Vec<Option<Column>>,
}
// enum represents variants of column values
#[derive(Debug, Clone)]
pub enum ColumnVal {
    One(String),
    Two(bool),
    Three(f64),
    Four(i64),
}
// error struct for error handling when reading csv
#[derive(Debug)]
struct MyError(String);

impl Error for MyError {}

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "There is an error: {}", self.0)
    }
}
impl DataFrame {
    pub fn new(col_num: usize) -> Self {
       DataFrame {columns: vec![None; col_num]}
    }

    // read-csv implemented from one of the old starter files, takes in file name, and numbers represents Column values
    pub fn read_csv(&mut self, path: &str, types: &Vec<u32>) -> Result<(), Box<dyn Error>> {
       
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path1 = PathBuf::from(manifest_dir).join(path.to_string());
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b',')
            .has_headers(false)
            .flexible(true)
            .from_path(path1)?;
        let mut first_row = true;
        let mut rows = vec![];
        let mut column_names = vec![];
        
        for result in rdr.records() {
            // Notice that we need to provide a type hint for automatic
            // deserialization.
            let r = result.unwrap();
            let mut row: Vec<ColumnVal> = vec![];
            

            if first_row {
                for elem in r.iter() {
                    // These are the labels
                    column_names.push(elem.to_string());
                }
                first_row = false;
                continue;
            }
            for (i, elem) in r.iter().enumerate() {
                match types[i] {
                    1 => row.push(ColumnVal::One(elem.to_string())),
                    2 => row.push(ColumnVal::Two(elem.parse::<bool>().unwrap())),
                    3 => row.push(ColumnVal::Three(elem.parse::<f64>().unwrap())),
                    4 => row.push(ColumnVal::Four(elem.parse::<i64>().unwrap())),
                    _ => return Err(Box::new(MyError("Unknown type".to_string()))),
                }
            }
            rows.push(row.clone());
            // Put the data into the dataframe
        }
        for i in 0..column_names.len() {
            let mut column_data = vec![];
            for j in 0..rows.len() {
                column_data.push(rows[j][i].clone());
            }
            let col = Column {label: column_names[i].clone(), data: column_data };
            self.columns[i] = Some(col);
        }
        Ok(())
    }

    

    
}