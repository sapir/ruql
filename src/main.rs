mod ast;
mod parser;

fn main() {
    let program = parser::parse(
        "
        q(x) = cities(name);
        ",
    )
    .unwrap();
    dbg!(program);
}
