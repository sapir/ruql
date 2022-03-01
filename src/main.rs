use anyhow::Result;

use crate::ast::{Clause, ColumnProjection, SourceClause};

mod ast;
mod compiler;
mod parser;

fn main() -> Result<()> {
    let program = parser::parse(
        "
        data cities(name, country) =
            (\"Jerusalem\", \"Israel\"),
            (\"Paris\", \"France\"),
            (\"London\", \"England\")
        ;

        israeli_cities(name) = cities(name, country), country = \"Israel\";
        ",
    )
    .unwrap();

    let prelude = compiler::Prelude::from(program);

    // TODO: parser
    let query = vec![Clause::Source(SourceClause {
        name: "israeli_cities".to_owned(),
        projection: vec![ColumnProjection {
            src: "name".to_owned(),
            dst: "name".to_owned(),
        }],
    })];

    let query = prelude.compile(query)?;

    println!("{}", query.to_sql());

    Ok(())
}
