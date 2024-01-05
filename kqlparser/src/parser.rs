use std::str::{self, FromStr};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_while1, escaped, is_a};
use nom::character::complete::{digit1, multispace0, multispace1, none_of, one_of};
use nom::combinator::{map, opt};
use nom::multi::{many0, separated_list0, separated_list1, fold_many0};
use nom::sequence::{tuple, preceded, delimited, separated_pair, terminated};
use nom::IResult;

use super::ast::{Expr, Literal, Operator, Query, Source, Type};
use super::is_kql_identifier;

fn parse_type(i: &str) -> IResult<&str, Type> {
    alt((
        map(tag("string"), |_| Type::String),
        map(tag("bool"), |_| Type::Bool),
        map(tag("int"), |_| Type::Int),
    ))(i)
}

fn parse_type_mapping(i: &str) -> IResult<&str, Vec<(String, Type)>> {
    separated_list1(tag(","), separated_pair(
        delimited(multispace0, map(take_identifier, |i| i.to_string()), multispace0),
        tag(":"),
        delimited(multispace0, parse_type, multispace0)
    ))(i)
}

fn take_identifier(i: &str) -> IResult<&str, &str> {
    let (input, identifier) = take_while1(is_kql_identifier)(i)?;

    // exclude reserved keywords
    if identifier == "by" {
        return Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::Tag)));
    }
    Ok((input, identifier))
}

fn parse_string(i: &str) -> IResult<&str, String> {
    map(alt((
        delimited(tag("\""), alt((escaped(none_of::<&str, _, _>("\\\""), '\\', tag("\"")), tag(""))), tag("\"")),
        delimited(tag("'"), alt((escaped(none_of::<&str, _, _>("\\'"), '\\', tag("'")), tag(""))), tag("'"))
    )), |s| s.to_string())(i)
}

fn parse_literal(i: &str) -> IResult<&str, Literal> {
    alt((
        map(tag("true"), |_| Literal::Bool(true)),
        map(tag("false"), |_| Literal::Bool(false)),
        map(tag("None"), |_| Literal::None),
        map(digit1, |x| Literal::Int(FromStr::from_str(x).unwrap())),
        map(parse_string, |s| Literal::String(s)),
    ))(i)
}

fn parse_ident(i: &str) -> IResult<&str, Expr> {
    alt((
        map(parse_literal, |l| Expr::Literal(l)),
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
            |(n, _, _, x, _)| Expr::Func(n.to_string(), x),
        ),
        map(take_identifier, |i| Expr::Ident(i.to_string())),
    ))(i)
}

fn parse_delim(i: &str) -> IResult<&str, Expr> {
    alt((
        map(
            tuple((tag("("), multispace0, parse_or, multispace0, tag(")"))),
            |(_, _, x, _, _)| x,
        ),
        parse_ident,
    ))(i)
}

fn parse_muldiv(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = parse_delim(i)?;
    fold_many0(tuple((multispace0, one_of("*/%"), multispace0, parse_delim)), move || initial.clone(), |acc, (_, o, _, g)| match o {
        '*' => Expr::Multiply(Box::new(acc), Box::new(g)),
        '/' => Expr::Divide(Box::new(acc), Box::new(g)),
        '%' => Expr::Modulo(Box::new(acc), Box::new(g)),
        _ => unreachable!()
    })(i)
}

fn parse_addsub(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = parse_muldiv(i)?;
    fold_many0(tuple((multispace0, one_of("+-"), multispace0, parse_muldiv)), move || initial.clone(), |acc, (_, o, _, g)| match o {
        '+' => Expr::Add(Box::new(acc), Box::new(g)),
        '-' => Expr::Substract(Box::new(acc), Box::new(g)),
        _ => unreachable!()
    })(i)
}

fn parse_predicate(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = parse_addsub(i)?;
    let (i, e) = fold_many0(tuple((multispace0, is_a("!=<>"), multispace0, parse_addsub)), move || Ok(initial.clone()), |acc, (_, o, _, g)| acc.and_then(|acc| Ok(match o {
        "==" => Expr::Equals(Box::new(acc), Box::new(g)),
        "!=" => Expr::NotEquals(Box::new(acc), Box::new(g)),
        "<" => Expr::Less(Box::new(acc), Box::new(g)),
        ">" => Expr::Greater(Box::new(acc), Box::new(g)),
        "<=" => Expr::LessOrEqual(Box::new(acc), Box::new(g)),
        ">=" => Expr::GreaterOrEqual(Box::new(acc), Box::new(g)),
        _ => return Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::Tag)))
    })))(i)?;
    Ok((i, e?))
}

fn parse_and(i: &str) -> IResult<&str, Expr> {
    alt((
        map(
            tuple((parse_delim, multispace0, tag("and"), multispace0, parse_or)),
            |(first, _, _, _, second)| Expr::And(Box::new(first), Box::new(second)),
        ),
        parse_predicate,
    ))(i)
}

fn parse_or(i: &str) -> IResult<&str, Expr> {
    alt((
        map(
            tuple((parse_and, multispace0, tag("or"), multispace0, parse_or)),
            |(first, _, _, _, second)| Expr::Or(Box::new(first), Box::new(second)),
        ),
        parse_and,
    ))(i)
}

pub fn parse_expr(i: &str) -> IResult<&str, Expr> {
    parse_or(i)
}

fn datatable_query(i: &str) -> IResult<&str, (Vec<(String, Type)>, Vec<Expr>)> {
    preceded(terminated(tag_no_case("datatable"), multispace1), separated_pair(
        delimited(tag("("), parse_type_mapping, tag(")")),
        multispace0,
        delimited(tag("["), separated_list1(tag(","), delimited(multispace0, parse_expr, multispace0)), tag("]"))
    ))(i)
}

fn extend_query(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    map(
        tuple((
            tag_no_case("extend"),
            multispace1,
            separated_list0(
                tuple((multispace0, tag(","), multispace0)),
                map(
                    tuple((take_while1(is_kql_identifier), multispace0, tag("="), multispace0, parse_expr)),
                    |(n, _, _, _, e)| (Some(n.to_string()), e)
                ),
            ),
        )),
        |(_, _, x)| x
    )(i)
}

fn externaldata_query(i: &str) -> IResult<&str, (Vec<(String, Type)>, Vec<String>)> {
    preceded(tuple((tag_no_case("externaldata"), multispace1)), separated_pair(
        delimited(tag("("), parse_type_mapping, tag(")")),
        multispace0,
        delimited(tag("["), separated_list1(tag(","), delimited(multispace0, parse_string, multispace0)), tag("]"))
    ))(i)
}

fn join_query(i: &str) -> IResult<&str, (Query, Vec<String>)> {
    map(
        tuple((
            tag_no_case("join"),
            multispace1,
            tag("("),
            parse_query,
            tag(")"),
            multispace1,
            tag("on"),
            multispace1,
            separated_list0(
                tag(","),
                map(
                    tuple((multispace0::<&str, _>, take_while1(is_kql_identifier), multispace0)),
                    |(_, e, _)| e.to_string(),
                ),
            ),
        )),
        |(_, _, _, a, _, _, _, _, g)| (a, g),
    )(i)
}

fn mv_expand_query(i: &str) -> IResult<&str, String> {
    map(
        tuple((
            tag_no_case("mv-expand"),
            multispace1,
            take_identifier,
        )),
        |(_, _, g)| g.to_string(),
    )(i)
}

fn project_query(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    map(
        tuple((
            tag_no_case("project"),
            multispace1,
            separated_list0(
                tuple((multispace0, tag(","), multispace0)),
                alt((
                    map(
                        tuple((take_while1(is_kql_identifier), multispace0, tag("="), multispace0, parse_expr)),
                        |(n, _, _, _, e)| (Some(n.to_string()), e)
                    ),
                    map(parse_expr, |e| (None, e))
                )),
            ),
        )),
        |(_, _, x)| x
    )(i)
}

fn where_query(i: &str) -> IResult<&str, Expr> {
    let (i, (_, _, e)) = tuple((tag_no_case("where"), multispace1, parse_expr))(i)?;
    Ok((i, e))
}

fn summarize_query(i: &str) -> IResult<&str, (Vec<Expr>, Vec<Expr>)> {
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

fn sort_query(i: &str) -> IResult<&str, Vec<String>> {
    preceded(tuple((tag_no_case("sort"), multispace1, tag_no_case("by"))), separated_list1(
        tag(","),
        map(
            tuple((multispace0::<&str, _>, take_while1(is_kql_identifier), multispace0)),
            |(_, e, _)| e.to_string(),
        ),
    ))(i)
}

fn take_query(i: &str) -> IResult<&str, u32> {
    map(
        tuple((
            alt((tag_no_case("take"), tag_no_case("limit"))),
            multispace1,
            map(digit1, |x| FromStr::from_str(x).unwrap()),
        )),
        |(_, _, t)| t,
    )(i)
}

fn parse_operator(i: &str) -> IResult<&str, Operator> {
    alt((
        map(extend_query, |e| Operator::Extend(e)),
        map(join_query, |(a, g)| Operator::Join(a, g)),
        map(mv_expand_query, |e| Operator::MvExpand(e)),
        map(project_query, |p| Operator::Project(p)),
        map(summarize_query, |(a, g)| Operator::Summarize(a, g)),
        map(sort_query, |o| Operator::Sort(o)),
        map(take_query, |t| Operator::Take(t)),
        map(where_query, |e| Operator::Where(e))
    ))(i)
}

fn parse_source(i: &str) -> IResult<&str, Source> {
    alt((
        map(datatable_query, |(a, g)| Source::Datatable(a, g)),
        map(externaldata_query, |(t, c)| Source::Externaldata(t, c)),
        map(take_while1(is_kql_identifier), |e: &str| Source::Reference(e.to_string()))
    ))(i)
}

pub fn parse_query(i: &str) -> IResult<&str, Query> {
    map(separated_pair(parse_source, multispace0, many0(preceded(terminated(tag("|"), multispace0), parse_operator))),
    |(source, operators)| Query {
        source,
        operators
    })(i)
}
