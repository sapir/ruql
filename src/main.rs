use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use rusqlite::types::ValueRef;
// TODO: separate the lib to its own crate so that rustyline isn't required as a
// dependency
use rustyline::{error::ReadlineError, Editor};

use ruql::{
    parse_program,
    parser::{parse_repl_stmt, ReplStmt},
    Prelude,
};
use structopt::StructOpt;

const PRELUDE_FILENAME: &str = "prelude.ruql";

fn handle_input(prelude: &mut Prelude, database: &rusqlite::Connection, code: &str) -> Result<()> {
    match parse_repl_stmt(code)? {
        ReplStmt::DataEntry(data_entry) => {
            prelude
                .add_data_entry(data_entry)
                .context("Adding data entry")?;
        }

        ReplStmt::Rule(rule) => {
            prelude.add_rule(rule).context("Adding rule")?;
        }

        ReplStmt::Query(query) => {
            let query = prelude
                .compile(database, query)
                .context("Compiling query to SQL")?;
            let sql = query.to_sql();

            let mut stmt = database
                .prepare_cached(&sql)
                .context("Preparing SQL for execution")?;
            let column_count = stmt.column_count();
            let mut rows = stmt.query([]).context("Executing query")?;
            while let Some(row) = rows.next().context("Retrieving row")? {
                println!(
                    "{}",
                    (0..column_count)
                        .map(|idx| {
                            Ok(match row.get_ref(idx)? {
                                ValueRef::Null => "NULL".to_string(),
                                ValueRef::Integer(x) => x.to_string(),
                                ValueRef::Real(x) => x.to_string(),
                                ValueRef::Text(x) => String::from_utf8_lossy(x).into_owned(),
                                ValueRef::Blob(_) => bail!("Blobs not supported"),
                            })
                        })
                        .collect::<Result<Vec<String>, _>>()?
                        .join(",")
                );
            }
        }
    }

    Ok(())
}

#[derive(StructOpt)]
struct Opts {
    database: Option<PathBuf>,
}

fn main() -> Result<()> {
    let opts = Opts::from_args();

    let database = if let Some(path) = opts.database {
        rusqlite::Connection::open(path)?
    } else {
        rusqlite::Connection::open_in_memory()?
    };

    // TODO: completer
    let mut editor = Editor::<()>::new();
    let mut prelude = if let Ok(code) = std::fs::read_to_string(PRELUDE_FILENAME) {
        let program = parse_program(&code)?;
        Prelude::from(program)
    } else {
        Prelude::default()
    };

    // TODO: save/restore readline history
    let mut input = String::new();
    loop {
        let readline = editor.readline(if input.trim().is_empty() {
            "ruql> "
        } else {
            " ...> "
        });

        match readline {
            Ok(line) => {
                input.push_str(line.trim());
                input.push('\n');
                if input.trim_end().ends_with(';') {
                    let input = std::mem::take(&mut input);

                    editor.add_history_entry(input.trim());

                    for input_part in input.split_inclusive(';') {
                        let input_part = input_part.trim();
                        if input_part.is_empty() {
                            continue;
                        }

                        match handle_input(&mut prelude, &database, &input_part) {
                            Ok(()) => {}
                            Err(e) => {
                                println!("Error: {:?}", e);
                            }
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => break,
            Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {}", err);
                break;
            }
        }
    }

    Ok(())
}
