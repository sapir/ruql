use anyhow::Result;
// TODO: separate the lib to its own crate so that rustyline isn't required as a
// dependency
use rustyline::{error::ReadlineError, Editor};

use ruql::{parse_program, parse_query, Prelude};

const PRELUDE_FILENAME: &str = "prelude.ruql";

fn handle_input(prelude: &Prelude, code: &str) -> Result<String> {
    // TODO: support definitions
    let query = parse_query(code)?;
    let query = prelude.compile(query)?;
    Ok(query.to_sql())
}

fn main() -> Result<()> {
    // TODO: completer
    let mut editor = Editor::<()>::new();
    let prelude = if let Ok(code) = std::fs::read_to_string(PRELUDE_FILENAME) {
        let program = parse_program(&code)?;
        Prelude::from(program)
    } else {
        Prelude::default()
    };

    // TODO: save/restore readline history
    let mut input = String::new();
    loop {
        let readline = editor.readline(if input.is_empty() { "ruql> " } else { " ...> " });

        match readline {
            Ok(line) => {
                input.push_str(line.trim());
                input.push('\n');
                if input.contains(';') {
                    let input = std::mem::take(&mut input);
                    editor.add_history_entry(input.trim());

                    match handle_input(&prelude, &input) {
                        Ok(sql) => {
                            println!("{}", sql);
                        }
                        Err(e) => {
                            println!("Error: {}", e);
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
