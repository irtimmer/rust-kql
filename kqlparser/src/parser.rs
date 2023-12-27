use std::str::{self, FromStr};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until, take_while1};
use nom::character::complete::{digit1, multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::multi::{many0, separated_list0, separated_list1, fold_many0};
use nom::sequence::tuple;
use nom::IResult;

use super::ast::{Expr, Operator, TabularExpression, Value};
use super::is_kql_identifier;

enum AddsubOperator {
    Add,
    Substract,
}

enum MuldivOperator {
    Multiply,
    Divide,
    Modulo,
}

enum PredicateOperator {
    Equals,
    NotEquals,
    Less,
    Greater,
    LessOrEqual,
    GreaterOrEqual
}

fn parse_predicate_op(i: &[u8]) -> IResult<&[u8], PredicateOperator> {
    alt((
        map(tag("=="), |_| PredicateOperator::Equals),
        map(tag("!="), |_| PredicateOperator::NotEquals),
        map(tag("<"), |_| PredicateOperator::Less),
        map(tag(">"), |_| PredicateOperator::Greater),
        map(tag("<="), |_| PredicateOperator::LessOrEqual),
        map(tag(">="), |_| PredicateOperator::GreaterOrEqual)
    ))(i)
}

fn parse_addsub_op(i: &[u8]) -> IResult<&[u8], AddsubOperator> {
    alt((
        map(tag("+"), |_| AddsubOperator::Add),
        map(tag("-"), |_| AddsubOperator::Substract),
    ))(i)
}

fn parse_muldiv_op(i: &[u8]) -> IResult<&[u8], MuldivOperator> {
    alt((
        map(tag("*"), |_| MuldivOperator::Multiply),
        map(tag("/"), |_| MuldivOperator::Divide),
        map(tag("%"), |_| MuldivOperator::Modulo),
    ))(i)
}

fn take_identifier(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, identifier) = take_while1(is_kql_identifier)(i)?;

    // exclude reserved keywords
    if identifier == b"by" {
        return Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::Tag)));
    }
    Ok((input, identifier))
}

fn parse_ident(i: &[u8]) -> IResult<&[u8], Expr> {
    alt((
        map(tag("true"), |_| Expr::Value(Value::Bool(true))),
        map(tag("false"), |_| Expr::Value(Value::Bool(false))),
        map(tag("None"), |_| Expr::Value(Value::None)),
        map(digit1, |x| {
            Expr::Value(Value::Int(
                FromStr::from_str(str::from_utf8(x).unwrap()).unwrap(),
            ))
        }),
        map(tuple((tag("'"), take_until("'"), tag("'"))), |(_, s, _)| {
            Expr::Value(Value::String(
                FromStr::from_str(str::from_utf8(s).unwrap()).unwrap(),
            ))
        }),
        map(
            tuple((tag("\""), take_until("\""), tag("\""))),
            |(_, s, _)| {
                Expr::Value(Value::String(
                    FromStr::from_str(str::from_utf8(s).unwrap()).unwrap(),
                ))
            },
        ),
        map(
            tuple((
                take_while1(is_kql_identifier),
                multispace0,
                tag("("),
                separated_list0(
                    tag(","),
                    map(tuple((multispace0, parse_expr, multispace0)), |(_, x, _)| x),
                ),
                tag(")"),
            )),
            |(n, _, _, x, _)| Expr::Func(FromStr::from_str(str::from_utf8(n).unwrap()).unwrap(), x),
        ),
        map(take_identifier, |i| {
            Expr::Ident(FromStr::from_str(str::from_utf8(i).unwrap()).unwrap())
        }),
    ))(i)
}

fn parse_delim(i: &[u8]) -> IResult<&[u8], Expr> {
    alt((
        map(
            tuple((tag("("), multispace0, parse_or, multispace0, tag(")"))),
            |(_, _, x, _, _)| x,
        ),
        parse_ident,
    ))(i)
}

fn parse_muldiv(i: &[u8]) -> IResult<&[u8], Expr> {
    let (i, initial) = parse_delim(i)?;
    fold_many0(tuple((multispace0, parse_muldiv_op, multispace0, parse_delim)), move || initial.clone(), |acc, (_, o, _, g)| match o {
        MuldivOperator::Multiply => Expr::Multiply(Box::new(acc), Box::new(g)),
        MuldivOperator::Divide => Expr::Divide(Box::new(acc), Box::new(g)),
        MuldivOperator::Modulo => Expr::Modulo(Box::new(acc), Box::new(g)),
    })(i)
}

fn parse_addsub(i: &[u8]) -> IResult<&[u8], Expr> {
    let (i, initial) = parse_muldiv(i)?;
    fold_many0(tuple((multispace0, parse_addsub_op, multispace0, parse_muldiv)), move || initial.clone(), |acc, (_, o, _, g)| match o {
        AddsubOperator::Add => Expr::Add(Box::new(acc), Box::new(g)),
        AddsubOperator::Substract => Expr::Substract(Box::new(acc), Box::new(g)),
    })(i)
}

fn parse_predicate(i: &[u8]) -> IResult<&[u8], Expr> {
    let (i, initial) = parse_addsub(i)?;
    fold_many0(tuple((multispace0, parse_predicate_op, multispace0, parse_addsub)), move || initial.clone(), |acc, (_, o, _, g)| match o {
        PredicateOperator::Equals => Expr::Equals(Box::new(acc), Box::new(g)),
        PredicateOperator::NotEquals => Expr::NotEquals(Box::new(acc), Box::new(g)),
        PredicateOperator::Less => Expr::Less(Box::new(acc), Box::new(g)),
        PredicateOperator::Greater => Expr::Greater(Box::new(acc), Box::new(g)),
        PredicateOperator::LessOrEqual => Expr::LessOrEqual(Box::new(acc), Box::new(g)),
        PredicateOperator::GreaterOrEqual => Expr::GreaterOrEqual(Box::new(acc), Box::new(g))
    })(i)
}

fn parse_and(i: &[u8]) -> IResult<&[u8], Expr> {
    alt((
        map(
            tuple((parse_delim, multispace0, tag("and"), multispace0, parse_or)),
            |(first, _, _, _, second)| Expr::And(Box::new(first), Box::new(second)),
        ),
        parse_predicate,
    ))(i)
}

fn parse_or(i: &[u8]) -> IResult<&[u8], Expr> {
    alt((
        map(
            tuple((parse_and, multispace0, tag("or"), multispace0, parse_or)),
            |(first, _, _, _, second)| Expr::Or(Box::new(first), Box::new(second)),
        ),
        parse_and,
    ))(i)
}

pub fn parse_expr(i: &[u8]) -> IResult<&[u8], Expr> {
    parse_or(i)
}

fn extend_query(i: &[u8]) -> IResult<&[u8], Vec<(Option<String>, Expr)>> {
    map(
        tuple((
            tag_no_case("extend"),
            multispace1,
            separated_list0(
                tuple((multispace0, tag(","), multispace0)),
                map(
                    tuple((take_while1(is_kql_identifier), multispace0, tag("="), multispace0, parse_expr)),
                    |(n, _, _, _, e)| (Some(FromStr::from_str(str::from_utf8(n).unwrap()).unwrap()), e)
                ),
            ),
        )),
        |(_, _, x)| x
    )(i)
}

fn join_query(i: &[u8]) -> IResult<&[u8], (TabularExpression, Vec<String>)> {
    map(
        tuple((
            tag_no_case("join"),
            multispace1,
            tag("("),
            parse_tabular,
            tag(")"),
            multispace1,
            tag("on"),
            multispace1,
            separated_list0(
                tag(","),
                map(
                    tuple((multispace0, take_while1(is_kql_identifier), multispace0)),
                    |(_, e, _)| FromStr::from_str(str::from_utf8(e).unwrap()).unwrap(),
                ),
            ),
        )),
        |(_, _, _, a, _, _, _, _, g)| (a, g),
    )(i)
}

fn project_query(i: &[u8]) -> IResult<&[u8], Vec<(Option<String>, Expr)>> {
    map(
        tuple((
            tag_no_case("project"),
            multispace1,
            separated_list0(
                tuple((multispace0, tag(","), multispace0)),
                alt((
                    map(
                        tuple((take_while1(is_kql_identifier), multispace0, tag("="), multispace0, parse_expr)),
                        |(n, _, _, _, e)| (Some(FromStr::from_str(str::from_utf8(n).unwrap()).unwrap()), e)
                    ),
                    map(parse_expr, |e| (None, e))
                )),
            ),
        )),
        |(_, _, x)| x
    )(i)
}

fn where_query(i: &[u8]) -> IResult<&[u8], Expr> {
    let (i, (_, _, e)) = tuple((tag_no_case("where"), multispace1, parse_expr))(i)?;
    Ok((i, e))
}

fn summarize_query(i: &[u8]) -> IResult<&[u8], (Vec<Expr>, Vec<Expr>)> {
    let (i, (a, g)) = map(
        tuple((
            tag_no_case("summarize"),
            multispace1,
            separated_list0(
                tag(","),
                map(tuple((multispace0, parse_expr, multispace0)), |(_, e, _)| e),
            ),
            opt(tuple((
                tag_no_case("by"),
                multispace1,
                separated_list1(
                    tag(","),
                    map(tuple((multispace0, parse_expr, multispace0)), |(_, e, _)| e),
                )
            )))
        )),
        |(_, _, a, g)| (a, g.map(|(_, _, g)| g).unwrap_or(vec![])),
    )(i)?;
    Ok((i, (a, g)))
}

fn take_query(i: &[u8]) -> IResult<&[u8], u32> {
    map(
        tuple((
            alt((tag_no_case("take"), tag_no_case("limit"))),
            multispace1,
            map(digit1, |x| {
                FromStr::from_str(str::from_utf8(x).unwrap()).unwrap()
            }),
        )),
        |(_, _, t)| t,
    )(i)
}

fn parse_operator(i: &[u8]) -> IResult<&[u8], Operator> {
    alt((
        map(extend_query, |e| Operator::Extend(e)),
        map(join_query, |(a, g)| Operator::Join(a, g)),
        map(project_query, |p| Operator::Project(p)),
        map(summarize_query, |(a, g)| Operator::Summarize(a, g)),
        map(take_query, |t| Operator::Take(t)),
        map(where_query, |e| Operator::Where(e))
    ))(i)
}

pub fn parse_tabular(i: &[u8]) -> IResult<&[u8], TabularExpression> {
    let (i, id) = take_while1(is_kql_identifier)(i)?;
    let (i, p) = many0(tuple((multispace0, tag("|"), multispace0, parse_operator)))(i)?;

    Ok((
        i,
        TabularExpression {
            name: FromStr::from_str(str::from_utf8(id).unwrap()).unwrap(),
            operators: p.into_iter().map(|(_, _, _, x)| x).collect(),
        },
    ))
}
