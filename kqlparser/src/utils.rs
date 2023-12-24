use nom::character::is_alphanumeric;

#[inline]
pub fn is_kql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == '_' as u8
}
