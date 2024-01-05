#[inline]
pub fn is_kql_identifier(chr: char) -> bool {
    chr.is_alphanumeric() || chr == '_'
}
