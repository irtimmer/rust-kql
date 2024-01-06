use std::str::{self, FromStr};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_while1, escaped, is_a};
use nom::character::complete::{digit1, multispace0, multispace1, none_of, one_of, hex_digit1};
use nom::character::streaming::{u64, i64};
use nom::combinator::{map, opt, recognize};
use nom::multi::{many0, separated_list0, separated_list1, fold_many0};
use nom::sequence::{tuple, preceded, delimited, separated_pair, terminated, pair};
use nom::IResult;

use super::ast::{Expr, Literal, Operator, Options, Query, Source, Type};
use super::{dec_to_i64, decimal, is_kql_identifier, trim};

fn parse_type(i: &str) -> IResult<&str, Type> {
    alt((
        map(tag("bool"), |_| Type::Bool),
        map(tag("int"), |_| Type::Int),
        map(tag("long"), |_| Type::Long),
        map(tag("string"), |_| Type::String),
        map(tag("timespan"), |_| Type::Timespan),
    ))(i)
}

fn parse_options(i: &str) -> IResult<&str, Options> {
    map(separated_list0(multispace1, separated_pair(
        parse_identifier,
        trim(tag("=")),
        parse_literal
    )), |x| x.into_iter().collect())(i)
}

fn parse_type_mapping(i: &str) -> IResult<&str, Vec<(String, Type)>> {
    separated_list1(tag(","), separated_pair(
        trim(parse_identifier),
        tag(":"),
        trim(parse_type)
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

fn parse_identifier(i: &str) -> IResult<&str, String> {
    map(take_identifier, |i| i.to_string())(i)
}

fn parse_string(i: &str) -> IResult<&str, String> {
    map(alt((
        delimited(tag("\""), alt((escaped(none_of::<&str, _, _>("\\\""), '\\', tag("\"")), tag(""))), tag("\"")),
        delimited(tag("'"), alt((escaped(none_of::<&str, _, _>("\\'"), '\\', tag("'")), tag(""))), tag("'"))
    )), |s| s.to_string())(i)
}

fn parse_bool(i: &str) -> IResult<&str, Option<bool>> {
    alt((
        map(tag_no_case("true"), |_| Some(true)),
        map(tag_no_case("false"), |_| Some(false)),
        map(recognize(pair(opt(tag("-")), digit1)), |x: &str| Some(x.parse::<i32>().unwrap() != 0)),
        map(tag("null"), |_| None)
    ))(i)
}

fn parse_int(i: &str) -> IResult<&str, Option<i32>> {
    alt((
        map(preceded(tag_no_case("0x"), hex_digit1), |x| Some(i32::from_str_radix(x, 16).unwrap())),
        map(recognize(pair(opt(tag("-")), digit1)), |x: &str| Some(x.parse().unwrap())),
        map(tag("null"), |_| None)
    ))(i)
}

fn parse_long(i: &str) -> IResult<&str, Option<i64>> {
    alt((
        map(preceded(tag_no_case("0x"), hex_digit1), |x| Some(i64::from_str_radix(x, 16).unwrap())),
        map(recognize(pair(opt(tag("-")), digit1)), |x: &str| Some(x.parse().unwrap())),
        map(tag("null"), |_| None)
    ))(i)
}

fn parse_timespan(i: &str) -> IResult<&str, Option<i64>> {
    alt((
        map(terminated(decimal, pair(multispace0, alt((tag("days"), tag("day"), tag("d"))))), |x| Some(dec_to_i64(x, 1000 * 1000 * 1000 * 60 * 60 * 24))),
        map(terminated(decimal, pair(multispace0, alt((tag("hours"), tag("hour"), tag("h"))))), |x| Some(dec_to_i64(x, 1000 * 1000 * 1000 * 60 * 60))),
        map(terminated(decimal, pair(multispace0, alt((tag("minutes"), tag("minute"), tag("m"))))), |x| Some(dec_to_i64(x, 1000 * 1000 * 1000 * 60))),
        map(terminated(decimal, pair(multispace0, alt((tag("seconds"), tag("second"), tag("s"))))), |x| Some(dec_to_i64(x, 1000 * 1000 * 1000))),
        map(terminated(decimal, pair(multispace0, alt((tag("milliseconds"), tag("millisecond"), tag("milli"), tag("ms"))))), |x| Some(dec_to_i64(x, 1000 * 1000))),
        map(terminated(decimal, pair(multispace0, alt((tag("microseconds"), tag("microsecond"), tag("micro"))))), |x| Some(dec_to_i64(x, 1000))),
        map(terminated(decimal, pair(multispace0, alt((tag("ticks"), tag("tick"))))), |x| Some(dec_to_i64(x, 100))),
        map(
            tuple((separated_pair(i64, tag("."), separated_pair(u64, tag(":"), u64)), opt(preceded(tag(":"), decimal)))),
            |((d, (h, m)), s)| Some(((d * 24 + h as i64) * 60 + m as i64) * (1000 * 1000 * 1000 * 60) + s.map(|x| dec_to_i64(x, 1000 * 1000 * 1000)).unwrap_or(0) as i64)
        ),
        map(
            tuple((separated_pair(u64, tag(":"), u64), opt(preceded(tag(":"), decimal)))),
            |((h, m), s)| Some((h as i64 * 60 + m as i64) * (1000 * 1000 * 1000 * 60) + s.map(|x| dec_to_i64(x, 1000 * 1000 * 1000)).unwrap_or(0) as i64)
        ),
        map(tag("null"), |_| None)
    ))(i)
}

fn parse_literal(i: &str) -> IResult<&str, Literal> {
    alt((
        map(preceded(tag("bool"), delimited(tag("("), trim(parse_bool), tag(")"))), |x| Literal::Bool(x)),
        map(preceded(tag("int"), delimited(tag("("), trim(parse_int), tag(")"))), |x| Literal::Int(x)),
        map(preceded(tag("long"), delimited(tag("("), trim(parse_long), tag(")"))), |x| Literal::Long(x)),
        map(preceded(alt((tag("timespan"), tag("time"))), delimited(tag("("), trim(parse_timespan), tag(")"))), |x| Literal::Timespan(x)),
        map(tag("true"), |_| Literal::Bool(Some(true))),
        map(tag("false"), |_| Literal::Bool(Some(false))),
        map(preceded(tag_no_case("0x"), hex_digit1), |x| Literal::Long(Some(i64::from_str_radix(x, 16).unwrap()))),
        map(terminated(decimal, alt((tag("days"), tag("day"), tag("d")))), |x| Literal::Timespan(Some(dec_to_i64(x, 1000 * 1000 * 1000 * 60 * 60 * 24)))),
        map(terminated(decimal, alt((tag("hours"), tag("hour"), tag("h")))), |x| Literal::Timespan(Some(dec_to_i64(x, 1000 * 1000 * 1000 * 60 * 60)))),
        map(terminated(decimal, alt((tag("minutes"), tag("minute"), tag("m")))), |x| Literal::Timespan(Some(dec_to_i64(x, 1000 * 1000 * 1000 * 60)))),
        map(terminated(decimal, alt((tag("seconds"), tag("second"), tag("s")))), |x| Literal::Timespan(Some(dec_to_i64(x, 1000 * 1000 * 1000)))),
        map(terminated(decimal, alt((tag("milliseconds"), tag("millisecond"), tag("milli"), tag("ms")))), |x| Literal::Timespan(Some(dec_to_i64(x, 1000 * 1000)))),
        map(terminated(decimal, alt((tag("microseconds"), tag("microsecond"), tag("micro")))), |x| Literal::Timespan(Some(dec_to_i64(x, 1000)))),
        map(terminated(decimal, alt((tag("ticks"), tag("tick")))), |x| Literal::Timespan(Some(dec_to_i64(x, 100)))),
        map(digit1, |x| Literal::Long(Some(FromStr::from_str(x).unwrap()))),
        map(parse_string, |s| Literal::String(s)),
    ))(i)
}

fn parse_ident(i: &str) -> IResult<&str, Expr> {
    alt((
        map(parse_literal, |l| Expr::Literal(l)),
        map(
            separated_pair(
                parse_identifier,
                multispace0,
                delimited(tag("("), separated_list0(
                    tag(","),
                    trim(parse_expr),
                ), tag(")"))
            ),
            |(n, x)| Expr::Func(n, x),
        ),
        map(parse_identifier, |i| Expr::Ident(i)),
    ))(i)
}

fn parse_delim(i: &str) -> IResult<&str, Expr> {
    alt((
        delimited(tag("("), trim(parse_or), tag(")")),
        parse_ident,
    ))(i)
}

fn parse_muldiv(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = parse_delim(i)?;
    fold_many0(pair(trim(one_of("*/%")), parse_delim), move || initial.clone(), |acc, (o, g)| match o {
        '*' => Expr::Multiply(Box::new(acc), Box::new(g)),
        '/' => Expr::Divide(Box::new(acc), Box::new(g)),
        '%' => Expr::Modulo(Box::new(acc), Box::new(g)),
        _ => unreachable!()
    })(i)
}

fn parse_addsub(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = parse_muldiv(i)?;
    fold_many0(pair(trim(one_of("+-")), parse_muldiv), move || initial.clone(), |acc, (o, g)| match o {
        '+' => Expr::Add(Box::new(acc), Box::new(g)),
        '-' => Expr::Substract(Box::new(acc), Box::new(g)),
        _ => unreachable!()
    })(i)
}

fn parse_predicate(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = parse_addsub(i)?;
    let (i, e) = fold_many0(pair(trim(is_a("!=<>")), parse_addsub), move || Ok(initial.clone()), |acc, (o, g)| acc.and_then(|acc| Ok(match o {
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
            separated_pair(parse_delim, trim(tag("and")), parse_or),
            |(first, second)| Expr::And(Box::new(first), Box::new(second)),
        ),
        parse_predicate,
    ))(i)
}

fn parse_or(i: &str) -> IResult<&str, Expr> {
    alt((
        map(
            separated_pair(parse_and, trim(tag("or")), parse_or),
            |(first, second)| Expr::Or(Box::new(first), Box::new(second)),
        ),
        parse_and,
    ))(i)
}

pub fn parse_expr(i: &str) -> IResult<&str, Expr> {
    parse_or(i)
}

fn as_query(i: &str) -> IResult<&str, (Options, String)> {
    preceded(terminated(tag_no_case("as"), multispace1), map(
        pair(opt(terminated(parse_options, multispace1)), parse_identifier),
        |(o, a)| (o.unwrap_or_default(), a)
    ))(i)
}

fn consume_query(i: &str) -> IResult<&str, Options> {
    preceded(terminated(tag_no_case("consume"), multispace1), parse_options)(i)
}

fn count_query(i: &str) -> IResult<&str, ()> {
    map(terminated(tag_no_case("count"), multispace1), |_| ())(i)
}

fn datatable_query(i: &str) -> IResult<&str, (Vec<(String, Type)>, Vec<Expr>)> {
    preceded(terminated(tag_no_case("datatable"), multispace1), separated_pair(
        delimited(tag("("), parse_type_mapping, tag(")")),
        multispace0,
        delimited(tag("["), separated_list1(tag(","), trim(parse_expr)), tag("]"))
    ))(i)
}

fn distinct_query(i: &str) -> IResult<&str, Vec<String>> {
    preceded(terminated(tag_no_case("distinct"), multispace1), separated_list1(
        tag(","),
        trim(parse_identifier)
    ))(i)
}

fn evaluate_query(i: &str) -> IResult<&str, (Options, String, Vec<Expr>)> {
    preceded(terminated(tag_no_case("evaluate"), multispace1), tuple((
        terminated(parse_options, multispace1),
        terminated(parse_identifier, multispace0),
        delimited(tag("("), separated_list0(tag(","), trim(parse_expr)), tag(")"))
    )))(i)
}

fn extend_query(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    preceded(terminated(tag_no_case("extend"), multispace1), separated_list0(
        tuple((multispace0, tag(","), multispace0)),
        map(separated_pair(parse_identifier, trim(tag("=")), parse_expr), |(n, e)| (Some(n), e)),
    ))(i)
}

fn externaldata_query(i: &str) -> IResult<&str, (Vec<(String, Type)>, Vec<String>)> {
    preceded(terminated(tag_no_case("externaldata"), multispace1), separated_pair(
        delimited(tag("("), parse_type_mapping, tag(")")),
        multispace0,
        delimited(tag("["), separated_list1(tag(","), trim(parse_string)), tag("]"))
    ))(i)
}

fn facet_query(i: &str) -> IResult<&str, (Vec<String>, Vec<Operator>)> {
    preceded(terminated(separated_pair(tag_no_case("facet"), multispace1, tag_no_case("by")), multispace1), pair(
        separated_list0(tag(","), trim(parse_identifier)),
        map(opt(preceded(terminated(tag("with"), multispace0), delimited(
            tag("("),
            separated_list1(tag("|"), trim(parse_operator)),
            tag(")")
        ))), |o| o.unwrap_or_default())
    ))(i)
}

fn getschema_query(i: &str) -> IResult<&str, ()> {
    map(terminated(tag_no_case("getschema"), multispace1), |_| ())(i)
}

fn join_query(i: &str) -> IResult<&str, (Options, Query, Vec<String>)> {
    preceded(terminated(tag_no_case("join"), multispace1), tuple((
        terminated(parse_options, multispace0),
        terminated(delimited(tag("("), parse_query, tag(")")), multispace0),
        preceded(
            terminated(tag("on"), multispace1),
            separated_list0(tag(","), trim(parse_identifier))
        )
    )))(i)
}

fn lookup_query(i: &str) -> IResult<&str, (Options, Query, Vec<String>)> {
    preceded(terminated(tag_no_case("lookup"), multispace1), tuple((
        terminated(parse_options, multispace0),
        terminated(delimited(tag("("), parse_query, tag(")")), multispace0),
        preceded(
            terminated(tag("on"), multispace1),
            separated_list0(tag(","), trim(parse_identifier))
        )
    )))(i)
}

fn mv_expand_query(i: &str) -> IResult<&str, String> {
    preceded(terminated(tag_no_case("mv-expand"), multispace1), parse_identifier)(i)
}

fn project_query(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    preceded(terminated(tag_no_case("project"), multispace1), separated_list0(
        tag(","),
        trim(alt((
            map(separated_pair(parse_identifier, trim(tag("=")), parse_expr), |(n, e)| (Some(n), e)),
            map(parse_expr, |e| (None, e))
        ))),
    ))(i)
}

fn project_away_query(i: &str) -> IResult<&str, Vec<String>> {
    preceded(terminated(tag_no_case("project-away"), multispace1), separated_list1(
        tag(","),
        trim(parse_identifier)
    ))(i)
}

fn project_keep_query(i: &str) -> IResult<&str, Vec<String>> {
    preceded(terminated(tag_no_case("project-keep"), multispace1), separated_list1(
        tag(","),
        trim(parse_identifier)
    ))(i)
}

fn project_rename_query(i: &str) -> IResult<&str, Vec<(String, String)>> {
    preceded(terminated(tag_no_case("project-rename"), multispace1), separated_list1(
        tag(","),
        separated_pair(trim(parse_identifier), tag("="), trim(parse_identifier))
    ))(i)
}

fn where_query(i: &str) -> IResult<&str, Expr> {
    preceded(terminated(tag_no_case("where"), multispace1), parse_expr)(i)
}

fn sample_query(i: &str) -> IResult<&str, u32> {
    preceded(
        terminated(tag_no_case("sample"), multispace1),
        map(digit1, |x| FromStr::from_str(x).unwrap())
    )(i)
}

fn sample_distinct_query(i: &str) -> IResult<&str, (u32, String)> {
    preceded(
        terminated(tag_no_case("sample-distinct"), multispace1),
        separated_pair(
            map(digit1, |x| FromStr::from_str(x).unwrap()),
            delimited(multispace1, tag_no_case("by"), multispace1),
            parse_identifier
        )
    )(i)
}

fn serialize_query(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    preceded(terminated(tag_no_case("serialize"), multispace1), separated_list0(
        tag(","),
        trim(map(
            separated_pair(parse_identifier, trim(tag("=")), parse_expr),
            |(n, e)| (Some(n), e)
        )),
    ))(i)
}

fn summarize_query(i: &str) -> IResult<&str, (Vec<Expr>, Vec<Expr>)> {
    preceded(terminated(tag_no_case("summarize"), multispace1), pair(
        separated_list0(tag(","), trim(parse_expr)),
        map(opt(preceded(
            terminated(tag_no_case("by"), multispace1),
            separated_list1(tag(","), trim(parse_expr))
        )), |b| b.unwrap_or_default())
    ))(i)
}

fn sort_query(i: &str) -> IResult<&str, Vec<String>> {
    preceded(tuple((tag_no_case("sort"), multispace1, tag_no_case("by"))), separated_list1(
        tag(","),
        trim(parse_identifier)
    ))(i)
}

fn take_query(i: &str) -> IResult<&str, u32> {
    preceded(
        terminated(alt((tag_no_case("take"), tag_no_case("limit"))), multispace1),
        map(digit1, |x| FromStr::from_str(x).unwrap())
    )(i)
}

fn parse_operator(i: &str) -> IResult<&str, Operator> {
    alt((
        map(as_query, |(o, a)| Operator::As(o, a)),
        map(consume_query, |o| Operator::Consume(o)),
        map(count_query, |_| Operator::Count),
        map(distinct_query, |c| Operator::Distinct(c)),
        map(evaluate_query, |(o, n, x)| Operator::Evaluate(o, n, x)),
        map(extend_query, |e| Operator::Extend(e)),
        map(facet_query, |(a, g)| Operator::Facet(a, g)),
        map(getschema_query, |_| Operator::Getschema),
        map(join_query, |(o, a, g)| Operator::Join(o, a, g)),
        map(lookup_query, |(o, a, g)| Operator::Lookup(o, a, g)),
        map(mv_expand_query, |e| Operator::MvExpand(e)),
        alt((
            map(project_query, |p| Operator::Project(p)),
            map(project_away_query, |p| Operator::ProjectAway(p)),
            map(project_keep_query, |p| Operator::ProjectKeep(p)),
            map(project_rename_query, |p| Operator::ProjectRename(p))
        )),
        alt((
            map(sample_query, |s| Operator::Sample(s)),
            map(sample_distinct_query, |(s, c)| Operator::SampleDistinct(s, c))
        )),
        map(serialize_query, |e| Operator::Serialize(e)),
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
        map(parse_identifier, |e| Source::Reference(e))
    ))(i)
}

pub fn parse_query(i: &str) -> IResult<&str, Query> {
    map(separated_pair(parse_source, multispace0, many0(preceded(terminated(tag("|"), multispace0), parse_operator))),
    |(source, operators)| Query {
        source,
        operators
    })(i)
}
