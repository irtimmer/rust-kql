use arrow_array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow_array::builder::GenericStringBuilder;
use arrow_array::cast::AsArray;

use arrow_schema::{ArrowError, DataType};

use regex::Regex;

use std::sync::Arc;

pub fn regex_str_parse(regex: &str, array: &dyn Array) -> Result<Vec<Arc<dyn Array>>, ArrowError> {
    let re = Regex::new(&regex).map_err(|e| {
        ArrowError::ComputeError(format!("Regular expression did not compile: {e:?}"))
    })?;

    match array.data_type() {
        DataType::Utf8 => regex_parse(array.as_string::<i32>(), &re),
        DataType::LargeUtf8 => regex_parse(array.as_string::<i64>(), &re),
        _ => Err(ArrowError::ComputeError(
            "regex_str_parse() requires array to be either Utf8 or LargeUtf8".to_string(),
        )),
    }
}

pub fn regex_parse<OffsetSize: OffsetSizeTrait>(array: &GenericStringArray<OffsetSize>, regex: &Regex) -> Result<Vec<ArrayRef>, ArrowError> {
    let num_groups = regex.captures_len();
    let mut builders: Vec<GenericStringBuilder<OffsetSize>> = (1..num_groups)
        .map(|_| GenericStringBuilder::with_capacity(0, 0))
        .collect();

    array.iter().for_each(|value| match value {
        Some(value) => if let Some(captures) = regex.captures(value) {
            captures
                .iter()
                .skip(1)
                .zip(builders.iter_mut())
                .for_each(|(capture, builder)| builder.append_option(capture.map(|s| s.as_str())));
        } else {
            builders.iter_mut().for_each(GenericStringBuilder::append_null)
        }
        None => builders.iter_mut().for_each(GenericStringBuilder::append_null)
    });
    Ok(builders.into_iter().map(|mut b| Arc::new(b.finish()) as Arc<dyn Array>).collect())
}
