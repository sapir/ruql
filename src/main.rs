use anyhow::Result;

use ruql::{parse_program, parse_query, Prelude};

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

    let prelude = Prelude::from(program);

    let query = parse_query("israeli_cities(name);")?;
    let query = prelude.compile(query)?;
    println!("{}", query.to_sql());

    Ok(())
}
