use std::borrow::BorrowMut;

use anyhow::{Context, Result};
use pest::Parser as _;
use pest_derive::Parser;

use crate::ast::{
    Clause, ColumnProjection, ConditionClause, Identifier, Literal, Program, Rule as QueryRule,
    SourceClause,
};

#[derive(Parser)]
#[grammar = "ruql.pest"]
struct Parser;

type Pair<'a> = pest::iterators::Pair<'a, Rule>;
type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;

pub fn parse(code: &str) -> Result<Program> {
    let program = Parser::parse(Rule::program, code)
        .context("Failed to parse input")?
        .next()
        .unwrap();

    Ok(Program {
        rules: program
            .into_inner()
            .take_while(|pair| pair.as_rule() != Rule::EOI)
            .map(QueryRule::from)
            .collect(),
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
        assert_eq!(pair.as_rule(), Rule::string_literal);
        let interior = expect_next_rule(pair.into_inner(), Rule::string_interior);
        Self(interior.as_str().to_string())
    }
}

impl From<Pair<'_>> for QueryRule {
    fn from(pair: Pair<'_>) -> Self {
        let rule_pair = expect_next_rule(pair.into_inner(), Rule::rule);
        let mut pairs = rule_pair.into_inner();

        let lhs = expect_next_rule(&mut pairs, Rule::rule_lhs);
        let mut lhs_pairs = lhs.into_inner();
        let name = expect_identifier(&mut lhs_pairs);
        let columns = lhs_pairs.map(convert_identifier).collect();

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
