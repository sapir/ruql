pub type Identifier = String;
pub type RuleName = Identifier;
pub type ColumnName = Identifier;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Literal {
    String(String),
}

#[derive(Clone, Debug)]
pub struct Program {
    pub data_entries: Vec<DataEntry>,
    pub rules: Vec<Rule>,
}

#[derive(Clone, Debug)]
pub struct DataEntry {
    pub name: RuleName,
    pub columns: Vec<ColumnName>,
    pub tuples: Vec<Vec<Literal>>,
}

#[derive(Clone, Debug)]
pub struct Rule {
    pub name: RuleName,
    pub columns: Vec<ColumnName>,
    pub clauses: Vec<Clause>,
}

#[derive(Clone, Debug)]
pub enum Clause {
    Source(SourceClause),
    Condition(ConditionClause),
}

#[derive(Clone, Debug)]
pub struct SourceClause {
    pub name: RuleName,
    pub projection: Vec<ColumnProjection>,
}

#[derive(Clone, Debug)]
pub struct ColumnProjection {
    pub src: ColumnName,
    pub dst: ColumnName,
}

#[derive(Clone, Debug)]
pub struct ConditionClause {
    pub lhs: ColumnName,
    pub rhs: Literal,
}
