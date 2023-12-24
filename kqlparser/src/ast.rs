#[derive(Debug, PartialEq)]
pub struct TabularExpression {
    pub name: String,
    pub operators: Vec<Operator>
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Where(Expr)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Ident(String),
    Value(Value),
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
pub enum Value {
    None,
    String(String),
    Bool(bool),
    Int(u32)
}
