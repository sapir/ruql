mod ast;
mod parser;

fn main() {
    let program = parser::parse(
        "
        data cities(name, country) =
            (\"Jerusalem\", \"Israel\"),
            (\"Paris\", \"France\"),
            (\"London\", \"England\")
        ;

        q(name) = cities(name, country), country = \"Israel\";
        ",
    )
    .unwrap();
    dbg!(program);
}
