use hashbag::HashBag;
use num_bigint::BigInt;
use rusqlite::{types::ValueRef, Connection};
use std::iter::FromIterator;

use crate::{ast::Literal, parse_program, parse_query, Prelude};

const PRELUDE_BAZ_DATA_ENTRY: &str = r#"
data baz(a, b, c) =
    (1, 2, "aaa"),
    (1, 3, "bbb"),
;
"#;

fn setup_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "BEGIN;
        CREATE TABLE foo(a, b, c);
        CREATE TABLE bar(a, y, z);
        INSERT INTO foo(a, b, c) VALUES
            (1, 2, 'first'),
            (1, 3, 'second'),
            (2, 4, 'third'),
            (2, 5, 'fourth')
        ;
        INSERT INTO bar(a, y, z) VALUES
            (1, 3, 'hello'),
            (1, 5, 'world'),
            (3, -10, 'hi'),
            (2, 5, 'bye')
        ;
        COMMIT;",
    )
    .unwrap();
    conn
}

fn test_query(prelude_code: &str, code: &str, expected: &[&[Literal]]) {
    let prelude_code = prelude_code.trim();
    let prelude = if prelude_code.is_empty() {
        Prelude::default()
    } else {
        let prelude = parse_program(prelude_code).unwrap();
        Prelude::from(prelude)
    };

    let conn = setup_db();

    let query = parse_query(code).unwrap();
    let query = prelude.compile(&conn, query).unwrap();
    let sql = query.to_sql();
    println!("***** BEGIN SQL *****");
    println!("{}", sql);
    println!("***** END SQL *****");

    let mut stmt = conn.prepare_cached(&sql).unwrap();
    let column_count = stmt.column_count();
    let mut result_rows = stmt.query([]).unwrap();
    let result_rows = std::iter::from_fn(move || {
        let row = result_rows.next().unwrap()?;
        let row = (0..column_count)
            .map(|i| match row.get_ref(i).unwrap() {
                ValueRef::Integer(x) => Literal::Integer(x.into()),
                ValueRef::Text(s) => Literal::String(std::str::from_utf8(s).unwrap().to_owned()),
                value => panic!("unexpected value type {:?}", value),
            })
            .collect::<Vec<_>>();
        Some(row)
    })
    .collect::<Vec<_>>();

    let result_rows = HashBag::from_iter(result_rows.iter().map(|x| x.as_slice()));
    let expected = HashBag::from_iter(expected.iter().map(|x| *x));
    assert_eq!(result_rows, expected);
}

fn int(n: impl Into<BigInt>) -> Literal {
    Literal::Integer(n.into())
}

fn string(s: impl Into<String>) -> Literal {
    Literal::String(s.into())
}

#[test]
fn test_select_column() {
    test_query("", "foo(a);", &[&[int(1)], &[int(1)], &[int(2)], &[int(2)]]);
}

#[test]
fn test_select_all_columns() {
    test_query(
        "",
        "foo(a, b, c);",
        &[
            &[int(1), int(2), string("first")],
            &[int(1), int(3), string("second")],
            &[int(2), int(4), string("third")],
            &[int(2), int(5), string("fourth")],
        ],
    );
}

#[test]
fn test_single_data_entry_splat_in_rule_rhs() {
    let mut prelude = PRELUDE_BAZ_DATA_ENTRY.to_owned();
    prelude.push_str("tmp(a, b, c) = baz(..);");
    test_query(
        &prelude,
        "tmp(a, b, c);",
        &[
            &[int(1), int(2), string("aaa")],
            &[int(1), int(3), string("bbb")],
        ],
    );
}

#[test]
fn test_single_subquery_splat_in_rule_rhs() {
    let mut prelude = PRELUDE_BAZ_DATA_ENTRY.to_owned();
    prelude.push_str(
        "tmp(a, b, c) = baz(a, b, c);
        tmp2(a, b, c) = tmp(..);",
    );
    test_query(
        &prelude,
        "tmp2(a, b, c);",
        &[
            &[int(1), int(2), string("aaa")],
            &[int(1), int(3), string("bbb")],
        ],
    );
}

#[test]
fn test_single_external_table_splat_in_rule_rhs() {
    let mut prelude = PRELUDE_BAZ_DATA_ENTRY.to_owned();
    prelude.push_str("tmp(a, b, c) = foo(..);");
    test_query(
        &prelude,
        "tmp(a, b, c);",
        &[
            &[int(1), int(2), string("first")],
            &[int(1), int(3), string("second")],
            &[int(2), int(4), string("third")],
            &[int(2), int(5), string("fourth")],
        ],
    );
}

#[test]
fn test_single_data_entry_splat_in_rule_lhs() {
    let mut prelude = PRELUDE_BAZ_DATA_ENTRY.to_owned();
    // Invert baz order to check that we're splatting correctly.
    prelude.push_str("tmp(..) = baz(c, b, a);");
    test_query(
        &prelude,
        // Use splat here, too, because if we specify the column names then we
        // don't know if the splat added unnecessary columns that we didn't
        // specify.
        "tmp(..);",
        &[
            &[string("aaa"), int(2), int(1)],
            &[string("bbb"), int(3), int(1)],
        ],
    );
}

#[test]
fn test_single_subquery_splat_in_rule_lhs() {
    let mut prelude = PRELUDE_BAZ_DATA_ENTRY.to_owned();
    prelude.push_str(
        // See comment in test_single_data_entry_splat_in_rule_lhs about column
        // order inversion.
        "tmp(a, b, c) = baz(a, b, c);
        tmp2(..) = tmp(c, b, a);",
    );
    test_query(
        &prelude,
        // See comment in test_single_data_entry_splat_in_rule_lhs about why we
        // use the splat operator here.
        "tmp2(..);",
        &[
            &[string("aaa"), int(2), int(1)],
            &[string("bbb"), int(3), int(1)],
        ],
    );
}

#[test]
fn test_single_external_table_splat_in_rule_lhs() {
    test_query(
        // See comment in test_single_data_entry_splat_in_rule_lhs about column
        // order inversion.
        "tmp(..) = foo(c, b, a);",
        // See comment in test_single_data_entry_splat_in_rule_lhs about why we
        // use the splat operator here.
        "tmp(..);",
        &[
            &[string("first"), int(2), int(1)],
            &[string("second"), int(3), int(1)],
            &[string("third"), int(4), int(2)],
            &[string("fourth"), int(5), int(2)],
        ],
    );
}

#[test]
fn test_single_data_entry_splat_in_query_rhs() {
    test_query(
        PRELUDE_BAZ_DATA_ENTRY,
        "baz(..);",
        &[
            &[int(1), int(2), string("aaa")],
            &[int(1), int(3), string("bbb")],
        ],
    );
}

#[test]
fn test_single_subquery_splat_in_query_rhs() {
    let mut prelude = PRELUDE_BAZ_DATA_ENTRY.to_owned();
    prelude.push_str("tmp(a, b, c) = baz(a, b, c);");
    test_query(
        &prelude,
        "tmp(..);",
        &[
            &[int(1), int(2), string("aaa")],
            &[int(1), int(3), string("bbb")],
        ],
    );
}

#[test]
fn test_single_external_table_splat_in_query_rhs() {
    test_query(
        "",
        "foo(..);",
        &[
            &[int(1), int(2), string("first")],
            &[int(1), int(3), string("second")],
            &[int(2), int(4), string("third")],
            &[int(2), int(5), string("fourth")],
        ],
    );
}

#[test]
fn test_join_two_tables() {
    test_query(
        "",
        "foo(a, b, c), bar(a, y, z);",
        &[
            &[int(1), int(2), string("first"), int(3), string("hello")],
            &[int(1), int(2), string("first"), int(5), string("world")],
            &[int(1), int(3), string("second"), int(3), string("hello")],
            &[int(1), int(3), string("second"), int(5), string("world")],
            &[int(2), int(4), string("third"), int(5), string("bye")],
            &[int(2), int(5), string("fourth"), int(5), string("bye")],
        ],
    );
}

#[test]
fn test_join_with_splat_in_rule() {
    test_query(
        "tmp(a, b, c, y, z) = foo(..), bar(..);",
        "tmp(a, b, c, y, z);",
        &[
            &[int(1), int(2), string("first"), int(3), string("hello")],
            &[int(1), int(2), string("first"), int(5), string("world")],
            &[int(1), int(3), string("second"), int(3), string("hello")],
            &[int(1), int(3), string("second"), int(5), string("world")],
            &[int(2), int(4), string("third"), int(5), string("bye")],
            &[int(2), int(5), string("fourth"), int(5), string("bye")],
        ],
    );
}

#[test]
fn test_join_two_tables_with_extra_join_projection_condition() {
    test_query(
        "",
        "foo(a, b, c), bar(a, y: b, z);",
        &[
            &[int(1), int(3), string("second"), string("hello")],
            &[int(2), int(5), string("fourth"), string("bye")],
        ],
    );
}

#[test]
fn test_integer_literal_projection_condition() {
    test_query("", "foo(a, b: 2, c);", &[&[int(1), string("first")]]);
}

#[test]
fn test_integer_literal_condition_clause() {
    test_query(
        "",
        "foo(a, b, c), b = 2;",
        &[&[int(1), int(2), string("first")]],
    );
}

#[test]
fn test_join_table_with_self() {
    test_query(
        "",
        "foo(a, b), foo(a: b, b: c);",
        &[&[int(1), int(2), int(4)], &[int(1), int(2), int(5)]],
    );
}

#[test]
fn test_recursive() {
    test_query(
        "tmp(a) = foo(a);
        tmp(a) = tmp(a: x), foo(a: x, b: a);",
        "tmp(a);",
        &[&[int(1)], &[int(2)], &[int(3)], &[int(4)], &[int(5)]],
    );
}

#[test]
#[ignore]
fn test_recursive_with_inverse_statement_order() {
    test_query(
        "tmp(a) = tmp(a: x), foo(a: x, b: a);
        tmp(a) = foo(a);",
        "tmp(a);",
        &[&[int(1)], &[int(2)], &[int(3)], &[int(4)], &[int(5)]],
    );
}
