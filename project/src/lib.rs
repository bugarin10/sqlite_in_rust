use rusqlite::{Connection, Result};
use csv::ReaderBuilder;
use std::error::Error;



pub fn create_db() -> Result<()> {
    let conn = Connection::open("happiness.db")?;

    conn.execute(
        "create table if not exists happiness_table (
             country text not null,
             log_gdp_pc numeric not null,
             social_support numeric not null,
             life_exp numeric not null,
             freedom numeric not null,
             generosity numeric not null,
             corruption numeric not null
         )",
        (),
    )?;

    Ok(())
}

pub fn fill_data() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open("happiness.db")?;

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path("happiness_data.csv")?;


    let mut counter = 0;

    while let Some(result) = reader.records().next() {
        let record = result?;
    
        let country_ = &record[0];
        let log_gdp_pc_ = &record[1];
        let social_support_ = &record[2];
        let life_exp_ = &record[3];
        let freedom_ = &record[4];
        let generosity_ = &record[5];
        let corruption_ = &record[6];
    
        if let Err(err) = conn.execute(
            "INSERT INTO happiness_table (country, log_gdp_pc, social_support, life_exp, freedom, generosity, corruption) values (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            &[&country_, &log_gdp_pc_, &social_support_, &life_exp_, &freedom_, &generosity_, &corruption_],
        ) {
            eprintln!("Error inserting row: {}", err);
        }
    
        counter += 1;
    }
    
    println!("Executed {} times", counter);

    Ok(())
}

pub fn use_query(statement: String) -> Result<()>{
    let conn = Connection::open("happiness.db")?;

    let mut stmt = conn.prepare(&statement.to_string())?;

    let mut rows = stmt.query([]).unwrap();

    while let Some(row) = rows.next()? {
        println!("{:?}", row);
    }

    Ok(())
}

#[test]
fn test_database_exists(){
    use std::path::Path;
    
    let path = Path::new("happiness.db");
    assert!(path.exists());
}

#[test]
fn test_query_works(){
    let query = "SELECT * FROM happiness_table WHERE country LIKE 'S%' AND life_exp > 60";
    let _ = use_query(query.to_string());
}