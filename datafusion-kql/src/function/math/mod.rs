use datafusion_expr::ScalarUDF;
use datafusion_functions::math::random;

use std::sync::Arc;

use crate::make_alias_udf_function;

make_alias_udf_function!(random(), rand);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        rand,
        "Returns a random value in the range 0.0 <= x < 1.0",
        arg1
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![rand()]
}
