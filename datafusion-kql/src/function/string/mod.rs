use datafusion_expr::ScalarUDF;
use datafusion_functions::regex::regexp_replace;
use datafusion_functions::string::{concat, lower, repeat, replace, split_part, upper};
use datafusion_functions::unicode::{character_length, strpos, substr};

use std::sync::Arc;

use crate::make_alias_udf_function;

make_alias_udf_function!(strpos(), indexof);
make_alias_udf_function!(replace(), replace_string);
make_alias_udf_function!(regexp_replace(), replace_regex);
make_alias_udf_function!(split_part(), split);
make_alias_udf_function!(concat(), strcat);
make_alias_udf_function!(character_length(), strlen);
make_alias_udf_function!(repeat(), strrep);
make_alias_udf_function!(substr(), substring);
make_alias_udf_function!(lower(), tolower);
make_alias_udf_function!(upper(), toupper);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        indexof,
        "Returns the position of the first occurrence of a substring in a string.",
        arg1 arg2
    ));
    export_functions!((
        replace_string,
        "Replaces all occurrences of a substring in a string with another substring.",
        arg1 arg2
    ));
    export_functions!((
        replace_regex,
        "Replaces all occurrences of a regex pattern in a string with another substring.",
        arg1 arg2
    ));
    export_functions!((
        split,
        "Splits a string into an array of substrings based on a delimiter.",
        arg1 arg2
    ));
    export_functions!((
        strcat,
        "Concatenates multiple strings into one.",
        args
    ));
    export_functions!((
        strlen,
        "Returns the length of a string.",
        arg1
    ));
    export_functions!((
        strrep,
        "Repeats a string a specified number of times.",
        arg1 arg2
    ));
    export_functions!((
        substring,
        "Extracts a substring from a string starting at a specified position.",
        arg1 arg2 arg3
    ));
    export_functions!((
        tolower,
        "Converts a string to lowercase.",
        arg1
    ));
    export_functions!((
        toupper,
        "Converts a string to uppercase.",
        arg1
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        indexof(),
        replace_string(),
        replace_regex(),
        split(),
        strcat(),
        strlen(),
        strrep(),
        substring(),
        tolower(),
        toupper()
    ]
}