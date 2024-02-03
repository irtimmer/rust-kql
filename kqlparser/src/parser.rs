use std::str::{self, FromStr};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_while1, escaped, is_a};
use nom::character::complete::{digit1, multispace0, multispace1, none_of, one_of, hex_digit1};
use nom::character::streaming::{u64, i64};
use nom::combinator::{map, opt, recognize};
use nom::multi::{many0, separated_list0, separated_list1, fold_many0, many1};
use nom::sequence::{tuple, preceded, delimited, separated_pair, terminated, pair};
use nom::IResult;

use super::ast::*;
use super::{dec_to_i64, decimal, is_kql_identifier, take_identifier, trim};

fn type_tag(i: &str) -> IResult<&str, Type> {
    alt((
        map(tag("bool"), |_| Type::Bool),
        map(tag("int"), |_| Type::Int),
        map(tag("long"), |_| Type::Long),
        map(tag("string"), |_| Type::String),
        map(tag("timespan"), |_| Type::Timespan),
    ))(i)
}

fn options(i: &str) -> IResult<&str, Options> {
    map(separated_list0(multispace1, separated_pair(
        identifier,
        trim(tag("=")),
        literal
    )), |x| x.into_iter().collect())(i)
}

fn pattern(i: &str) -> IResult<&str, Vec<PatternToken>> {
    many1(trim(alt((
        map(tag("*"), |_| PatternToken::Wildcard),
        map(string, |s| PatternToken::String(s)),
        map(
            pair(identifier, opt(preceded(trim(tag(":")), type_tag))),
            |(n, t)| PatternToken::Column(n, t)
        )
    ))))(i)
}

fn type_mapping(i: &str) -> IResult<&str, Vec<(String, Type)>> {
    separated_list1(tag(","), separated_pair(
        trim(identifier),
        tag(":"),
        trim(type_tag)
    ))(i)
}

fn identifier(i: &str) -> IResult<&str, String> {
    map(take_identifier, |i| i.to_string())(i)
}

fn string(i: &str) -> IResult<&str, String> {
    map(alt((
        delimited(tag("\""), alt((escaped(none_of::<&str, _, _>("\\\""), '\\', tag("\"")), tag(""))), tag("\"")),
        delimited(tag("'"), alt((escaped(none_of::<&str, _, _>("\\'"), '\\', tag("'")), tag(""))), tag("'"))
    )), |s| s.to_string())(i)
}

fn boolean(i: &str) -> IResult<&str, Option<bool>> {
    alt((
        map(tag_no_case("true"), |_| Some(true)),
        map(tag_no_case("false"), |_| Some(false)),
        map(recognize(pair(opt(tag("-")), digit1)), |x: &str| Some(x.parse::<i32>().unwrap() != 0)),
        map(tag("null"), |_| None)
    ))(i)
}

fn integer(i: &str) -> IResult<&str, Option<i32>> {
    alt((
        map(preceded(tag_no_case("0x"), hex_digit1), |x| Some(i32::from_str_radix(x, 16).unwrap())),
        map(recognize(pair(opt(tag("-")), digit1)), |x: &str| Some(x.parse().unwrap())),
        map(tag("null"), |_| None)
    ))(i)
}

fn long(i: &str) -> IResult<&str, Option<i64>> {
    alt((
        map(preceded(tag_no_case("0x"), hex_digit1), |x| Some(i64::from_str_radix(x, 16).unwrap())),
        map(recognize(pair(opt(tag("-")), digit1)), |x: &str| Some(x.parse().unwrap())),
        map(tag("null"), |_| None)
    ))(i)
}

fn timespan(i: &str) -> IResult<&str, Option<i64>> {
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

fn literal(i: &str) -> IResult<&str, Literal> {
    alt((
        map(preceded(tag("bool"), delimited(tag("("), trim(boolean), tag(")"))), |x| Literal::Bool(x)),
        map(preceded(tag("int"), delimited(tag("("), trim(integer), tag(")"))), |x| Literal::Int(x)),
        map(preceded(tag("long"), delimited(tag("("), trim(long), tag(")"))), |x| Literal::Long(x)),
        map(preceded(alt((tag("timespan"), tag("time"))), delimited(tag("("), trim(timespan), tag(")"))), |x| Literal::Timespan(x)),
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
        map(string, |s| Literal::String(s)),
    ))(i)
}

fn ident_expr(i: &str) -> IResult<&str, Expr> {
    alt((
        map(literal, |l| Expr::Literal(l)),
        map(
            separated_pair(
                identifier,
                multispace0,
                delimited(tag("("), separated_list0(
                    tag(","),
                    trim(expr),
                ), tag(")"))
            ),
            |(n, x)| Expr::Func(n, x),
        ),
        map(identifier, |i| Expr::Ident(i)),
    ))(i)
}

fn delim_expr(i: &str) -> IResult<&str, Expr> {
    alt((
        delimited(tag("("), trim(or_expr), tag(")")),
        ident_expr,
    ))(i)
}

fn muldiv_expr(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = delim_expr(i)?;
    fold_many0(pair(trim(one_of("*/%")), delim_expr), move || initial.clone(), |acc, (o, g)| match o {
        '*' => Expr::Multiply(Box::new(acc), Box::new(g)),
        '/' => Expr::Divide(Box::new(acc), Box::new(g)),
        '%' => Expr::Modulo(Box::new(acc), Box::new(g)),
        _ => unreachable!()
    })(i)
}

fn addsub_expr(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = muldiv_expr(i)?;
    fold_many0(pair(trim(one_of("+-")), muldiv_expr), move || initial.clone(), |acc, (o, g)| match o {
        '+' => Expr::Add(Box::new(acc), Box::new(g)),
        '-' => Expr::Substract(Box::new(acc), Box::new(g)),
        _ => unreachable!()
    })(i)
}

fn predicate(i: &str) -> IResult<&str, Expr> {
    let (i, initial) = addsub_expr(i)?;
    let (i, e) = fold_many0(pair(trim(is_a("!=<>")), addsub_expr), move || Ok(initial.clone()), |acc, (o, g)| acc.and_then(|acc| Ok(match o {
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

fn and_expr(i: &str) -> IResult<&str, Expr> {
    alt((
        map(
            separated_pair(delim_expr, trim(tag("and")), or_expr),
            |(first, second)| Expr::And(Box::new(first), Box::new(second)),
        ),
        predicate,
    ))(i)
}

fn or_expr(i: &str) -> IResult<&str, Expr> {
    alt((
        map(
            separated_pair(and_expr, trim(tag("or")), or_expr),
            |(first, second)| Expr::Or(Box::new(first), Box::new(second)),
        ),
        and_expr,
    ))(i)
}

pub fn expr(i: &str) -> IResult<&str, Expr> {
    or_expr(i)
}

fn as_operator(i: &str) -> IResult<&str, (Options, String)> {
    preceded(terminated(tag_no_case("as"), multispace1), map(
        pair(opt(terminated(options, multispace1)), identifier),
        |(o, a)| (o.unwrap_or_default(), a)
    ))(i)
}

fn consume_operator(i: &str) -> IResult<&str, Options> {
    preceded(terminated(tag_no_case("consume"), multispace1), options)(i)
}

fn count_operator(i: &str) -> IResult<&str, ()> {
    map(terminated(tag_no_case("count"), multispace1), |_| ())(i)
}

fn datatable_operator(i: &str) -> IResult<&str, (Vec<(String, Type)>, Vec<Expr>)> {
    preceded(terminated(tag_no_case("datatable"), multispace1), separated_pair(
        delimited(tag("("), type_mapping, tag(")")),
        multispace0,
        delimited(tag("["), separated_list1(tag(","), trim(expr)), tag("]"))
    ))(i)
}

fn distinct_operator(i: &str) -> IResult<&str, Vec<String>> {
    preceded(terminated(tag_no_case("distinct"), multispace1), separated_list1(
        tag(","),
        trim(identifier)
    ))(i)
}

fn evaluate_operator(i: &str) -> IResult<&str, (Options, String, Vec<Expr>)> {
    preceded(terminated(tag_no_case("evaluate"), multispace1), tuple((
        terminated(options, multispace1),
        terminated(identifier, multispace0),
        delimited(tag("("), separated_list0(tag(","), trim(expr)), tag(")"))
    )))(i)
}

fn extend_operator(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    preceded(terminated(tag_no_case("extend"), multispace1), separated_list0(
        tuple((multispace0, tag(","), multispace0)),
        map(separated_pair(identifier, trim(tag("=")), expr), |(n, e)| (Some(n), e)),
    ))(i)
}

fn externaldata_operator(i: &str) -> IResult<&str, (Vec<(String, Type)>, Vec<String>)> {
    preceded(terminated(tag_no_case("externaldata"), multispace1), separated_pair(
        delimited(tag("("), type_mapping, tag(")")),
        multispace0,
        delimited(tag("["), separated_list1(tag(","), trim(string)), tag("]"))
    ))(i)
}

fn facet_operator(i: &str) -> IResult<&str, (Vec<String>, Vec<Operator>)> {
    preceded(terminated(separated_pair(tag_no_case("facet"), multispace1, tag_no_case("by")), multispace1), pair(
        separated_list0(tag(","), trim(identifier)),
        map(opt(preceded(terminated(tag("with"), multispace0), delimited(
            tag("("),
            separated_list1(tag("|"), trim(operator)),
            tag(")")
        ))), |o| o.unwrap_or_default())
    ))(i)
}

fn find_operator(i: &str) -> IResult<&str, (Options, (Option<Vec<Source>>, Expr), FindProjection)> {
    preceded(terminated(tag_no_case("find"), multispace1), tuple((
        terminated(options, multispace0),
        alt((
            map(separated_pair(
                preceded(
                    terminated(tag("in"), multispace1), 
                    delimited(tag("("), separated_list1(tag(","), trim(source)), tag(")"))
                ),
                multispace1,
                preceded(terminated(tag("where"), multispace1), expr)
            ), |(s, e)| (Some(s), e)),
            map(expr, |e| (None, e))
        )),
        map(opt(preceded(multispace1, alt((
            map(tag("project-smart"), |_| FindProjection::ProjectSmart),
            map(preceded(terminated(tag("project"), multispace1), separated_list1(trim(tag(",")), identifier)), |c| FindProjection::Project(c))
        )))), |x| x.unwrap_or(FindProjection::ProjectSmart))
    )))(i)
}

fn fork_operator(i: &str) -> IResult<&str, Vec<(Option<String>, Vec<Operator>)>> {
    preceded(terminated(tag_no_case("fork"), multispace1), separated_list1(
        tag(","),
        trim(alt((
            map(separated_pair(
                identifier,
                trim(tag("=")),
                delimited(tag("("), separated_list1(tag("|"), trim(operator)), tag(")"))
            ), |(n, e)| (Some(n), e)),
            map(delimited(tag("("), separated_list1(tag("|"), trim(operator)), tag(")")), |e| (None, e))
        )))
    ))(i)
}

fn getschema_operator(i: &str) -> IResult<&str, ()> {
    map(terminated(tag_no_case("getschema"), multispace1), |_| ())(i)
}

fn join_operator(i: &str) -> IResult<&str, (Options, Query, Vec<String>)> {
    preceded(terminated(tag_no_case("join"), multispace1), tuple((
        terminated(options, multispace0),
        terminated(delimited(tag("("), parse_query, tag(")")), multispace0),
        preceded(
            terminated(tag("on"), multispace1),
            separated_list0(tag(","), trim(identifier))
        )
    )))(i)
}

fn lookup_operator(i: &str) -> IResult<&str, (Options, Query, Vec<String>)> {
    preceded(terminated(tag_no_case("lookup"), multispace1), tuple((
        terminated(options, multispace0),
        terminated(delimited(tag("("), parse_query, tag(")")), multispace0),
        preceded(
            terminated(tag("on"), multispace1),
            separated_list0(tag(","), trim(identifier))
        )
    )))(i)
}

fn mv_expand_operator(i: &str) -> IResult<&str, String> {
    preceded(terminated(tag_no_case("mv-expand"), multispace1), identifier)(i)
}

fn parse_operator(i: &str) -> IResult<&str, (Options, Expr, Vec<PatternToken>)> {
    preceded(terminated(tag_no_case("parse"), multispace1), tuple((
        terminated(options, multispace0),
        terminated(expr, multispace0),
        preceded(terminated(tag("with"), multispace1), pattern)
    )))(i)
}

fn print_operator(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    preceded(terminated(tag_no_case("print"), multispace0), separated_list0(
        trim(tag(",")),
        map(separated_pair(identifier, trim(tag("=")), expr), |(n, e)| (Some(n), e)),
    ))(i)
}

fn project_operator(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    preceded(terminated(tag_no_case("project"), multispace1), separated_list0(
        tag(","),
        trim(alt((
            map(separated_pair(identifier, trim(tag("=")), expr), |(n, e)| (Some(n), e)),
            map(expr, |e| (None, e))
        ))),
    ))(i)
}

fn project_away_operator(i: &str) -> IResult<&str, Vec<String>> {
    preceded(terminated(tag_no_case("project-away"), multispace1), separated_list1(
        tag(","),
        trim(identifier)
    ))(i)
}

fn project_keep_operator(i: &str) -> IResult<&str, Vec<String>> {
    preceded(terminated(tag_no_case("project-keep"), multispace1), separated_list1(
        tag(","),
        trim(identifier)
    ))(i)
}

fn project_rename_operator(i: &str) -> IResult<&str, Vec<(String, String)>> {
    preceded(terminated(tag_no_case("project-rename"), multispace1), separated_list1(
        tag(","),
        separated_pair(trim(identifier), tag("="), trim(identifier))
    ))(i)
}

fn where_operator(i: &str) -> IResult<&str, Expr> {
    preceded(terminated(tag_no_case("where"), multispace1), expr)(i)
}

fn range_operator(i: &str) -> IResult<&str, (String, Expr, Expr, Expr)> {
    preceded(terminated(tag_no_case("range"), multispace1), tuple((
        terminated(identifier, multispace1),
        terminated(preceded(terminated(tag_no_case("from"), multispace1), expr), multispace1),
        terminated(preceded(terminated(tag_no_case("to"), multispace1), expr), multispace1),
        preceded(terminated(tag_no_case("step"), multispace1), expr)
    )))(i)
}

fn sample_operator(i: &str) -> IResult<&str, u32> {
    preceded(
        terminated(tag_no_case("sample"), multispace1),
        map(digit1, |x| FromStr::from_str(x).unwrap())
    )(i)
}

fn sample_distinct_operator(i: &str) -> IResult<&str, (u32, String)> {
    preceded(
        terminated(tag_no_case("sample-distinct"), multispace1),
        separated_pair(
            map(digit1, |x| FromStr::from_str(x).unwrap()),
            delimited(multispace1, tag_no_case("by"), multispace1),
            identifier
        )
    )(i)
}

fn serialize_operator(i: &str) -> IResult<&str, Vec<(Option<String>, Expr)>> {
    preceded(terminated(tag_no_case("serialize"), multispace1), separated_list0(
        tag(","),
        trim(map(
            separated_pair(identifier, trim(tag("=")), expr),
            |(n, e)| (Some(n), e)
        )),
    ))(i)
}

fn summarize_operator(i: &str) -> IResult<&str, (Vec<Expr>, Vec<Expr>)> {
    preceded(terminated(tag_no_case("summarize"), multispace1), pair(
        separated_list0(tag(","), trim(expr)),
        map(opt(preceded(
            terminated(tag_no_case("by"), multispace1),
            separated_list1(tag(","), trim(expr))
        )), |b| b.unwrap_or_default())
    ))(i)
}

fn sort_operator(i: &str) -> IResult<&str, Vec<String>> {
    preceded(tuple((tag_no_case("sort"), multispace1, tag_no_case("by"))), separated_list1(
        tag(","),
        trim(identifier)
    ))(i)
}

fn take_operator(i: &str) -> IResult<&str, u32> {
    preceded(
        terminated(alt((tag_no_case("take"), tag_no_case("limit"))), multispace1),
        map(digit1, |x| FromStr::from_str(x).unwrap())
    )(i)
}

fn union_operator(i: &str) -> IResult<&str, (Options, Vec<Source>)> {
    preceded(terminated(tag_no_case("union"), multispace1), tuple((
        terminated(options, multispace0),
        separated_list1(trim(tag(",")), alt((
            delimited(tag("("), trim(source), tag(")")),
            map(identifier, |e| Source::Reference(e))
        )))
    )))(i)
}

fn operator(i: &str) -> IResult<&str, Operator> {
    alt((
        map(as_operator, |(o, a)| Operator::As(o, a)),
        map(consume_operator, |o| Operator::Consume(o)),
        map(count_operator, |_| Operator::Count),
        map(distinct_operator, |c| Operator::Distinct(c)),
        map(evaluate_operator, |(o, n, x)| Operator::Evaluate(o, n, x)),
        map(extend_operator, |e| Operator::Extend(e)),
        map(facet_operator, |(a, g)| Operator::Facet(a, g)),
        map(fork_operator, |f| Operator::Fork(f)),
        map(getschema_operator, |_| Operator::Getschema),
        map(join_operator, |(o, a, g)| Operator::Join(o, a, g)),
        map(lookup_operator, |(o, a, g)| Operator::Lookup(o, a, g)),
        map(mv_expand_operator, |e| Operator::MvExpand(e)),
        alt((
            map(project_operator, |p| Operator::Project(p)),
            map(project_away_operator, |p| Operator::ProjectAway(p)),
            map(project_keep_operator, |p| Operator::ProjectKeep(p)),
            map(project_rename_operator, |p| Operator::ProjectRename(p))
        )),
        map(parse_operator, |(o, e, p)| Operator::Parse(o, e, p)),
        alt((
            map(sample_operator, |s| Operator::Sample(s)),
            map(sample_distinct_operator, |(s, c)| Operator::SampleDistinct(s, c))
        )),
        map(serialize_operator, |e| Operator::Serialize(e)),
        map(summarize_operator, |(a, g)| Operator::Summarize(a, g)),
        map(sort_operator, |o| Operator::Sort(o)),
        map(take_operator, |t| Operator::Take(t)),
        map(union_operator, |(o, s)| Operator::Union(o, s)),
        map(where_operator, |e| Operator::Where(e))
    ))(i)
}

fn source(i: &str) -> IResult<&str, Source> {
    alt((
        map(datatable_operator, |(a, g)| Source::Datatable(a, g)),
        map(externaldata_operator, |(t, c)| Source::Externaldata(t, c)),
        map(find_operator, |(o, (s, e), p)| Source::Find(o, s, e, p)),
        map(print_operator, |e| Source::Print(e)),
        map(range_operator, |(c, f, t, s)| Source::Range(c, f, t, s)),
        map(union_operator, |(o, s)| Source::Union(o, s)),
        map(identifier, |e| Source::Reference(e))
    ))(i)
}

pub fn parse_query(i: &str) -> IResult<&str, Query> {
    map(separated_pair(source, multispace0, many0(preceded(terminated(tag("|"), multispace0), operator))),
    |(source, operators)| Query {
        source,
        operators
    })(i)
}
