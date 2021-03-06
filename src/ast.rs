use num_bigint::BigInt;

pub type Identifier = String;
pub type RuleName = Identifier;
pub type ColumnName = Identifier;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Literal {
    Integer(BigInt),
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
    pub projection: Projection,
}

#[derive(Clone, Debug)]
pub struct Projection {
    pub columns: Vec<ColumnProjection>,
    pub has_splat: bool,
}

#[derive(Clone, Debug)]
pub struct ColumnProjection {
    pub src: ColumnName,
    pub dst: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Column(ColumnName),
    Literal(Literal),
}

#[derive(Clone, Debug)]
pub struct ConditionClause {
    pub lhs: ColumnName,
    pub rhs: Literal,
}
