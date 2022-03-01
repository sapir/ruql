pub type Identifier = String;
pub type RuleName = Identifier;
pub type ColumnName = Identifier;

#[derive(Clone, Debug)]
pub struct Program {
    pub rules: Vec<Rule>,
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
