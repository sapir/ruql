use std::collections::{hash_map, HashMap};

use anyhow::{anyhow, Result};
use itertools::Itertools;

use crate::ast::{
    Clause, ColumnName, ColumnProjection, ConditionClause, DataEntry, Literal, Program, Rule,
    RuleName, SourceClause,
};

#[derive(Clone, Debug, Default)]
pub struct Prelude {
    data_entries: HashMap<RuleName, DataEntry>,
    rules: HashMap<RuleName, Vec<Rule>>,
}

impl From<Program> for Prelude {
    fn from(program: Program) -> Self {
        let data_entries = program
            .data_entries
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect();

        // TODO: verify that column names are consistent
        // TODO: verify that rule names are disjoint from data entry names
        let rules = program
            .rules
            .into_iter()
            .into_grouping_map_by(|rule| rule.name.clone())
            .collect();

        Self {
            data_entries,
            rules,
        }
    }
}

impl Prelude {
    pub fn compile(&self, query: Vec<Clause>) -> Result<FullQuery> {
        let columns = auto_detect_columns(&query);

        let query = Rule {
            name: "query".to_owned(),
            columns,
            clauses: query,
        };

        QueryBuilder::compile(self, query)
    }
}

pub fn auto_detect_columns(clauses: &[Clause]) -> Vec<ColumnName> {
    clauses
        .iter()
        .filter_map(|clause| match clause {
            Clause::Source(source) => Some(source),
            Clause::Condition(_) => None,
        })
        .flat_map(|source| &source.projection)
        .map(|projection| &projection.dst)
        .unique()
        .cloned()
        .collect()
}

#[derive(Clone, Debug)]
pub struct Query {
    pub projection: Vec<SourcedColumn>,
    pub sources: Vec<ProjectedSource>,
    pub selection: Vec<Condition>,
}

impl Query {
    pub fn is_recursive(&self) -> bool {
        self.sources
            .iter()
            .any(|source| matches!(source.source, Source::RecurseToSelf))
    }

    pub fn to_sql(&self, self_id: Option<WithClauseId>) -> String {
        let mut s = String::new();
        s.push_str("SELECT ");
        // TODO: verify this at compilation time
        assert!(!self.projection.is_empty());
        s.push_str(
            &self
                .projection
                .iter()
                .copied()
                .map(|column| {
                    let (source_index, column) = self.get_column(column);
                    let mut proj_str =
                        format!("{}.{}", Self::source_name(source_index), column.src);
                    if column.src != column.dst {
                        proj_str.push_str(" AS ");
                        proj_str.push_str(&column.dst);
                    }
                    proj_str
                })
                .join(", "),
        );

        if !self.sources.is_empty() {
            s.push_str("\nFROM ");
            s.push_str(
                &self
                    .sources
                    .iter()
                    .enumerate()
                    .map(|(source_index, source)| {
                        let orig_name = match &source.source {
                            Source::NamedTable(name) => name.clone(),
                            Source::WithClause(id) => {
                                assert_ne!(self_id, Some(*id));
                                FullQuery::with_clause_name(*id)
                            }
                            Source::RecurseToSelf => FullQuery::with_clause_name(
                                self_id.expect("recursive query missing self id"),
                            ),
                        };
                        let new_name = Self::source_name(source_index);
                        format!("{} AS {}", orig_name, new_name)
                    })
                    .join(", "),
            );
        }

        if !self.selection.is_empty() {
            s.push_str("\nWHERE ");
            s.push_str(
                &self
                    .selection
                    .iter()
                    .map(|condition| condition.to_sql(self))
                    .join("\n  AND "),
            );
        }

        s
    }

    pub fn source_name(index: usize) -> String {
        format!("__src{}", index + 1)
    }

    pub fn get_column(&self, id: SourcedColumn) -> (usize, &ColumnProjection) {
        let SourcedColumn {
            source_index,
            column_index,
        } = id;
        let source = &self.sources[source_index];
        let column = &source.projection[column_index];
        (source_index, column)
    }

    fn get_column_as_src(&self, id: SourcedColumn) -> String {
        let (source_index, column) = self.get_column(id);
        format!("{}.{}", Self::source_name(source_index), column.src)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SourcedColumn {
    pub source_index: usize,
    pub column_index: usize,
}

#[derive(Clone, Debug)]
pub struct ProjectedSource {
    pub source: Source,
    pub projection: Vec<ColumnProjection>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WithClauseId(pub usize);

#[derive(Clone, Debug)]
pub enum Source {
    NamedTable(RuleName),
    WithClause(WithClauseId),
    RecurseToSelf,
}

#[derive(Clone, Debug)]
pub struct Condition {
    pub lhs: Value,
    pub rhs: Value,
}

impl Condition {
    pub fn to_sql(&self, query: &Query) -> String {
        let Self { lhs, rhs } = self;
        let lhs = lhs.to_sql(query);
        let rhs = rhs.to_sql(query);
        format!("{} = {}", lhs, rhs)
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    ColumnValue(SourcedColumn),
    Literal(Literal),
}

impl Value {
    pub fn to_sql(&self, query: &Query) -> String {
        match self {
            Value::ColumnValue(col) => query.get_column_as_src(*col),
            Value::Literal(s) => format!("'{}'", s.0.replace('\'', "''")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryUnion(pub Vec<Query>);

impl From<Query> for QueryUnion {
    fn from(q: Query) -> Self {
        Self(vec![q])
    }
}

impl QueryUnion {
    pub fn is_recursive(&self) -> bool {
        self.0.iter().any(|q| q.is_recursive())
    }

    pub fn to_sql(&self, self_id: Option<WithClauseId>) -> String {
        self.0
            .iter()
            .map(|query| query.to_sql(self_id))
            .join("\nUNION\n")
    }
}

#[derive(Clone, Debug)]
pub struct FullQuery {
    pub with_clauses: Vec<QueryUnion>,
    pub main_query: QueryUnion,
}

impl FullQuery {
    pub fn with_clause_name(id: WithClauseId) -> String {
        format!("__sq{}", id.0 + 1)
    }

    pub fn to_sql(&self) -> String {
        let mut s = String::new();

        if !self.with_clauses.is_empty() {
            s.push_str("WITH ");
            for (i, with_clause) in self.with_clauses.iter().enumerate() {
                let id = WithClauseId(i);

                if i > 0 {
                    s.push_str(", ");
                }
                if with_clause.is_recursive() {
                    s.push_str("RECURSIVE ");
                }
                s.push_str(&Self::with_clause_name(id));
                s.push_str(" AS (\n");
                s.push_str(&with_clause.to_sql(Some(id)));
                s.push_str("\n)");
            }
            s.push('\n');
        }

        if self.main_query.is_recursive() {
            // This isn't currently possible, but if it is in the future, then
            // the main query should be converted to a with clause. The main
            // query can simply be a 'select * from ...' query.
            todo!("main query is recursive!");
        }

        s.push_str(&self.main_query.to_sql(None));

        s
    }
}

#[derive(Clone, Debug, Default)]
struct QueryBuilder {
    with_clauses: Vec<QueryUnion>,
}

impl QueryBuilder {
    fn compile_rule(&mut self, prelude: &Prelude, rule: &Rule) -> Result<Query> {
        let Rule {
            name: rule_name,
            columns,
            clauses,
        } = rule;

        let mut column_map: HashMap<ColumnName, SourcedColumn> = HashMap::new();
        let mut sources = vec![];
        let mut selection = vec![];

        // First handle all the source clauses, so we can build the column map,
        // then we can handle the conditions, as conditions are allowed to
        // reference columns from sources that follow them.
        for clause in clauses {
            match clause {
                Clause::Source(SourceClause {
                    name: source_name,
                    projection,
                }) => {
                    // TODO: detect (and prevent/support) multilevel recursion
                    let source = if source_name == rule_name {
                        Source::RecurseToSelf
                    } else {
                        self.compile_subquery(prelude, source_name)?
                    };

                    // Once we push this source, its index will be equal to the
                    // old length.
                    let source_index = sources.len();

                    let projection = projection
                        .iter()
                        .enumerate()
                        .filter_map(|(i, column_projection)| {
                            let column_id = SourcedColumn {
                                source_index,
                                column_index: i,
                            };

                            match column_map.entry(column_projection.dst.clone()) {
                                hash_map::Entry::Vacant(vacant) => {
                                    vacant.insert(column_id);

                                    Some(column_projection.clone())
                                }

                                hash_map::Entry::Occupied(occupied) => {
                                    // A duplicate column means the query wants
                                    // the 2 columns to be equal.
                                    selection.push(Condition {
                                        lhs: Value::ColumnValue(*occupied.get()),
                                        rhs: Value::ColumnValue(column_id),
                                    });

                                    None
                                }
                            }
                        })
                        .collect();

                    sources.push(ProjectedSource { source, projection });
                }

                Clause::Condition(_) => {}
            }
        }

        for clause in clauses {
            match clause {
                Clause::Source(_) => {}

                Clause::Condition(ConditionClause { lhs, rhs }) => {
                    let lhs = *column_map
                        .get(lhs)
                        .ok_or_else(|| anyhow!("Unknown column {:?}", lhs))?;

                    selection.push(Condition {
                        lhs: Value::ColumnValue(lhs),
                        rhs: Value::Literal(rhs.clone()),
                    });
                }
            }
        }

        let projection = columns
            .iter()
            .map(|column| {
                column_map
                    .get(column)
                    .ok_or_else(|| anyhow!("Unknown column {:?}", column))
                    .copied()
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Query {
            projection,
            sources,
            selection,
        })
    }

    fn compile_subquery(&mut self, prelude: &Prelude, name: &str) -> Result<Source> {
        if let Some(_) = prelude.data_entries.get(name) {
            assert!(!prelude.rules.contains_key(name));

            // TODO: generate the data instead
            Ok(Source::NamedTable(name.to_owned()))
        } else if let Some(rules) = prelude.rules.get(name) {
            let union = QueryUnion(
                rules
                    .iter()
                    .map(|rule| self.compile_rule(prelude, rule))
                    .collect::<Result<Vec<Query>, _>>()?,
            );
            let index = self.with_clauses.len();
            self.with_clauses.push(union);
            Ok(Source::WithClause(WithClauseId(index)))
        } else {
            Err(anyhow!("Unknown rule name {:?}", name))
        }
    }

    fn compile(prelude: &Prelude, rule: Rule) -> Result<FullQuery> {
        let mut zelf = QueryBuilder::default();
        let main_query = zelf.compile_rule(prelude, &rule)?;
        Ok(FullQuery {
            with_clauses: zelf.with_clauses,
            main_query: main_query.into(),
        })
    }
}
