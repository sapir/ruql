use anyhow::Result;
// TODO: separate the lib to its own crate so that rustyline isn't required as a
// dependency
use rustyline::{error::ReadlineError, Editor};

use ruql::{
    parse_program,
    parser::{parse_repl_stmt, ReplStmt},
    Prelude,
};

const PRELUDE_FILENAME: &str = "prelude.ruql";

fn handle_input(prelude: &mut Prelude, code: &str) -> Result<()> {
    match parse_repl_stmt(code)? {
        ReplStmt::DataEntry(data_entry) => {
            prelude.add_data_entry(data_entry)?;
        }

        ReplStmt::Rule(rule) => {
            prelude.add_rule(rule)?;
        }

        ReplStmt::Query(query) => {
            let query = prelude.compile(query)?;
            println!("{}", query.to_sql());
        }
    }

    Ok(())
}

fn main() -> Result<()> {
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
        let readline = editor.readline(if input.is_empty() { "ruql> " } else { " ...> " });

        match readline {
            Ok(line) => {
                input.push_str(line.trim());
                input.push('\n');
                if input.contains(';') {
                    // TODO: split by ';'
                    let input = std::mem::take(&mut input);
                    editor.add_history_entry(input.trim());

                    match handle_input(&mut prelude, &input) {
                        Ok(()) => {}
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
