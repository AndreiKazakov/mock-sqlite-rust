use anyhow::{bail, Result};

use sqlite_starter_rust::db::DB;
use sqlite_starter_rust::sql::Select;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let db = DB::new(&args[1])?;
    let command = &args[2];

    match command.as_str() {
        ".dbinfo" => println!("number of tables: {}", db.tables()?.len()),
        ".tables" => println!("{}", db.tables()?.join(" ")),
        query if query.to_lowercase().starts_with("select count(*)") => {
            println!("{}", db.count(query.split(' ').last().unwrap())?)
        }
        query if query.to_lowercase().starts_with("select") => {
            let select = Select::parse_select(query)?;
            println!(
                "{}",
                db.select(select)?
                    .into_iter()
                    .map(|vs| vs
                        .into_iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join("|"))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
