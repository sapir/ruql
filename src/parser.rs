use std::borrow::BorrowMut;

use anyhow::{Context, Result};
use pest::Parser as _;
use pest_derive::Parser;

use crate::ast::{
    Clause, ColumnName, ColumnProjection, ConditionClause, DataEntry, Identifier, Literal, Program,
    Rule as QueryRule, RuleName, SourceClause,
};

#[derive(Parser)]
#[grammar = "ruql.pest"]
struct Parser;

type Pair<'a> = pest::iterators::Pair<'a, Rule>;
type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;

fn parse_input(rule: Rule, input: &str) -> Result<Pair> {
    Ok(Parser::parse(rule, input)
        .context("Failed to parse input")?
        .next()
        .unwrap())
}

pub fn parse_program(code: &str) -> Result<Program> {
    let program = parse_input(Rule::program, code)?;

    let mut data_entries = vec![];
    let mut rules = vec![];

    for pair in program.into_inner() {
        match pair.as_rule() {
            Rule::program_entry => {
                let pair = pair.into_inner().next().unwrap();
                match pair.as_rule() {
                    Rule::data_entry => {
                        data_entries.push(pair.into());
                    }

                    Rule::rule => {
                        rules.push(pair.into());
                    }

                    _ => unreachable!(),
                }
            }

            Rule::EOI => {
                break;
            }

            _ => unreachable!(),
        }
    }

    Ok(Program {
        data_entries,
        rules,
    })
}

pub fn parse_query(code: &str) -> Result<Vec<Clause>> {
    let pair = parse_input(Rule::rule_clauses, code)?;

    Ok(pair.into_inner().map(Clause::from).collect())
}

pub enum ReplStmt {
    DataEntry(DataEntry),
    Rule(QueryRule),
    Query(Vec<Clause>),
}

pub fn parse_repl_stmt(code: &str) -> Result<ReplStmt> {
    let pair = parse_input(Rule::repl_stmt, code)?;
    let pair = pair.into_inner().next().unwrap();
    Ok(match pair.as_rule() {
        Rule::program_entry => {
            let pair = pair.into_inner().next().unwrap();
            match pair.as_rule() {
                Rule::data_entry => ReplStmt::DataEntry(pair.into()),
                Rule::rule => ReplStmt::Rule(pair.into()),
                _ => unreachable!(),
            }
        }

        Rule::rule_clauses => ReplStmt::Query(pair.into_inner().map(Clause::from).collect()),

        _ => unreachable!(),
    })
}

fn expect_next_rule<'a, P: BorrowMut<Pairs<'a>>>(mut pairs: P, rule: Rule) -> Pair<'a> {
    let pair = pairs.borrow_mut().next().expect("missing pair");
    assert_eq!(pair.as_rule(), rule);
    pair
}

fn convert_identifier(pair: Pair) -> Identifier {
    assert_eq!(pair.as_rule(), Rule::identifier);
    pair.as_str().to_string()
}

fn expect_identifier<'a, P: BorrowMut<Pairs<'a>>>(pairs: P) -> Identifier {
    convert_identifier(expect_next_rule(pairs, Rule::identifier))
}

impl From<Pair<'_>> for Literal {
    fn from(pair: Pair<'_>) -> Self {
        assert_eq!(pair.as_rule(), Rule::literal);
        let pair = pair.into_inner().next().unwrap();
        match pair.as_rule() {
            Rule::integer_literal => Literal::Integer(
                pair.as_str()
                    .parse()
                    .expect("failed to convert integer to bignum"),
            ),

            Rule::string_literal => {
                let interior = expect_next_rule(pair.into_inner(), Rule::string_interior);
                Literal::String(interior.as_str().to_string())
            }

            _ => unreachable!("{:?}", pair),
        }
    }
}

struct RuleLhs {
    name: RuleName,
    columns: Vec<ColumnName>,
}

impl From<Pair<'_>> for RuleLhs {
    fn from(pair: Pair<'_>) -> Self {
        assert_eq!(pair.as_rule(), Rule::rule_lhs);
        let mut pairs = pair.into_inner();
        let name = expect_identifier(&mut pairs);
        let columns = pairs.map(convert_identifier).collect();
        Self { name, columns }
    }
}

impl From<Pair<'_>> for DataEntry {
    fn from(pair: Pair<'_>) -> Self {
        let mut pairs = pair.into_inner();
        let RuleLhs { name, columns } = pairs.next().unwrap().into();
        let rhs = expect_next_rule(pairs, Rule::data_rhs);
        let tuples = rhs
            .into_inner()
            .map(|pair| {
                assert_eq!(pair.as_rule(), Rule::data_tuple);
                pair.into_inner().map(Literal::from).collect()
            })
            .collect();
        Self {
            name,
            columns,
            tuples,
        }
    }
}

impl From<Pair<'_>> for QueryRule {
    fn from(pair: Pair<'_>) -> Self {
        let mut pairs = pair.into_inner();

        let RuleLhs { name, columns } = pairs.next().unwrap().into();

        let clauses = expect_next_rule(&mut pairs, Rule::rule_clauses);
        let clauses = clauses.into_inner().map(Clause::from).collect();

        Self {
            name,
            columns,
            clauses,
        }
    }
}

impl From<Pair<'_>> for Clause {
    fn from(pair: Pair<'_>) -> Self {
        let pair = pair.into_inner().next().unwrap();

        match pair.as_rule() {
            Rule::src_clause => Clause::Source(SourceClause::from(pair)),

            Rule::condition_clause => Clause::Condition(ConditionClause::from(pair)),

            _ => unreachable!(),
        }
    }
}

impl From<Pair<'_>> for SourceClause {
    fn from(pair: Pair<'_>) -> Self {
        let mut pairs = pair.into_inner();
        let name = expect_identifier(&mut pairs);

        let params = expect_next_rule(pairs, Rule::src_clause_params);
        let projection = params
            .into_inner()
            .map(|pair| {
                let name = convert_identifier(pair);
                ColumnProjection {
                    src: name.clone(),
                    dst: name,
                }
            })
            .collect();

        Self { name, projection }
    }
}

impl From<Pair<'_>> for ConditionClause {
    fn from(pair: Pair<'_>) -> Self {
        let mut pairs = pair.into_inner();
        let lhs = expect_identifier(&mut pairs);
        let rhs = Literal::from(pairs.next().unwrap());
        Self { lhs, rhs }
    }
}
