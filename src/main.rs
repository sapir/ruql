mod ast;
mod parser;

fn main() {
    let program = parser::parse(
        "
        q(name) = cities(name, country), country = \"Israel\";
        ",
    )
    .unwrap();
    dbg!(program);
}
