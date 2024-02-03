use std::collections::HashMap;

pub type Options = HashMap<String, Literal>;

#[derive(Debug, PartialEq)]
pub struct Query {
    pub source: Source,
    pub operators: Vec<Operator>
}

#[derive(Debug, PartialEq)]
pub enum Source {
    Datatable(Vec<(String, Type)>, Vec<Expr>),
    Externaldata(Vec<(String, Type)>, Vec<String>),
    Find(Options, Option<Vec<Source>>, Expr, FindProjection),
    Print(Vec<(Option<String>, Expr)>),
    Range(String, Expr, Expr, Expr),
    Reference(String),
    Union(Options, Vec<Source>)
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    As(Options, String),
    Consume(Options),
    Count,
    Distinct(Vec<String>),
    Evaluate(Options, String, Vec<Expr>),
    Extend(Vec<(Option<String>, Expr)>),
    Facet(Vec<String>, Vec<Operator>),
    Getschema,
    Join(Options, Query, Vec<String>),
    Lookup(Options, Query, Vec<String>),
    MvExpand(String),
    Parse(Options, Expr, Vec<PatternToken>),
    Project(Vec<(Option<String>, Expr)>),
    ProjectAway(Vec<String>),
    ProjectKeep(Vec<String>),
    ProjectRename(Vec<(String, String)>),
    Sample(u32),
    SampleDistinct(u32, String),
    Serialize(Vec<(Option<String>, Expr)>),
    Summarize(Vec<Expr>, Vec<Expr>),
    Sort(Vec<String>),
    Take(u32),
    Union(Options, Vec<Source>),
    Where(Expr)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Ident(String),
    Literal(Literal),
    Equals(Box<Expr>, Box<Expr>),
    NotEquals(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Add(Box<Expr>, Box<Expr>),
    Substract(Box<Expr>, Box<Expr>),
    Multiply(Box<Expr>, Box<Expr>),
    Divide(Box<Expr>, Box<Expr>),
    Modulo(Box<Expr>, Box<Expr>),
    Less(Box<Expr>, Box<Expr>),
    Greater(Box<Expr>, Box<Expr>),
    LessOrEqual(Box<Expr>, Box<Expr>),
    GreaterOrEqual(Box<Expr>, Box<Expr>),
    Func(String, Vec<Expr>)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Bool,
    Int,
    Long,
    String,
    Timespan
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Bool(Option<bool>),
    Int(Option<i32>),
    Long(Option<i64>),
    String(String),
    Timespan(Option<i64>)
}

#[derive(Debug, PartialEq)]
pub enum FindProjection {
    ProjectSmart,
    Project(Vec<String>)
}

#[derive(Debug, PartialEq)]
pub enum PatternToken {
    Wildcard,
    String(String),
    Column(String, Option<Type>),
}
