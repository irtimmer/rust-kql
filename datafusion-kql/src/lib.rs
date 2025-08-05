pub mod function;
pub mod planner;
mod operators;
mod session;

#[macro_use]
pub mod macros;

pub use operators::*;
pub use session::*;

use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;

use log::debug;

use std::sync::Arc;

/// Fluent-style API for creating `Expr`s
#[allow(unused)]
pub mod expr_fn {
    pub use super::function::math::expr_fn::*;
    pub use super::function::string::expr_fn::*;
}

/// Returns all default scalar functions
pub fn all_default_scalar_functions() -> Vec<Arc<ScalarUDF>> {
    function::math::functions()
        .into_iter()
        .chain(function::string::functions())
        .collect::<Vec<_>>()
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let scalar_functions: Vec<Arc<ScalarUDF>> = all_default_scalar_functions();
    scalar_functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    Ok(())
}
