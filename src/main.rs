use anyhow::Result;

use crate::parser::{parse_program, parse_query};

mod ast;
mod compiler;
mod parser;

fn main() -> Result<()> {
    let program = parse_program(
        "
        data cities(name, country) =
            (\"Jerusalem\", \"Israel\"),
            (\"Paris\", \"France\"),
            (\"London\", \"England\")
        ;

        israeli_cities(name) = cities(name, country), country = \"Israel\";
        ",
    )?;

    let prelude = compiler::Prelude::from(program);

    let query = parse_query("israeli_cities(name);")?;
    let query = prelude.compile(query)?;
    println!("{}", query.to_sql());

    Ok(())
}
