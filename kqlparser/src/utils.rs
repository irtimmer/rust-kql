use nom::character::complete::multispace0;
use nom::error::ParseError;
use nom::sequence::delimited;
use nom::{IResult, InputLength, Parser, InputTake, InputIter, InputTakeAtPosition, AsChar};

#[inline]
pub fn is_kql_identifier(chr: char) -> bool {
    chr.is_alphanumeric() || chr == '_'
}

pub fn trim<I, O, E, F>(f: F) -> impl FnMut(I) -> IResult<I, O, E>
where
    I: Clone + InputLength + InputTake + InputIter,
    I: InputTakeAtPosition,
    <I as InputTakeAtPosition>::Item: AsChar + Clone,
    F: Parser<I, O, E>,
    E: ParseError<I>,
{
    delimited(multispace0, f, multispace0)
}
