#[macro_export]
macro_rules! make_alias_udf_function {
    ($UDF:expr, $NAME:ident) => {
        #[doc = concat!("Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ", stringify!($NAME))]
        pub fn $NAME() -> std::sync::Arc<datafusion_expr::ScalarUDF> {
            // Singleton instance of the function
            static INSTANCE: std::sync::LazyLock<
                std::sync::Arc<datafusion_expr::ScalarUDF>,
            > = std::sync::LazyLock::new(|| {
                std::sync::Arc::new(datafusion_expr::ScalarUDF::clone(&$UDF).with_aliases([stringify!($NAME)]))
            });
            std::sync::Arc::clone(&INSTANCE)
        }
    };
}