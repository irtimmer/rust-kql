use datafusion::arrow::datatypes::DataType;
use datafusion::config::ConfigOptions;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::SessionState;
use datafusion::execution::context::SessionContext;

use datafusion_common::{not_impl_err, plan_datafusion_err, DataFusionError, ResolvedTableReference, Result, TableReference};

use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
use datafusion_expr::planner::ContextProvider;
use datafusion_expr::registry::FunctionRegistry;

use kqlparser::ast::Statement;
use kqlparser::parser::parse;

use std::collections::HashMap;
use std::sync::Arc;

use crate::planner::KqlToRel;

#[allow(async_fn_in_trait)]
pub trait SessionContextExt {
    async fn kql(&self, sql: &str) -> Result<DataFrame>;
}

#[allow(async_fn_in_trait)]
pub trait SessionStateExt {
    async fn create_logical_plan_kql(&self, kql: &str) -> Result<LogicalPlan>;
    fn kql_to_statement(&self, kql: &str) -> Result<Statement>;
    async fn kql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan>;
}

impl SessionContextExt for SessionContext {
    async fn kql(&self, kql: &str) -> Result<DataFrame> {
        let plan = self.state().create_logical_plan_kql(kql).await?;
        self.execute_logical_plan(plan).await
    }
}

impl SessionStateExt for SessionState {
    async fn create_logical_plan_kql(&self, kql: &str) -> Result<LogicalPlan> {
        //let dialect = self.config.options().sql_parser.dialect.as_str();
        let statement = self.kql_to_statement(kql)?;
        let plan = self.kql_statement_to_plan(statement).await?;
        Ok(plan)
    }
    
    fn kql_to_statement(&self, kql: &str) -> Result<Statement> {
        let mut statements = parse(kql).unwrap().1;
        if statements.len() > 1 {
            return not_impl_err!(
                "The context currently only supports a single KQL statement"
            )
        }
        Ok(statements.pop().ok_or_else(|| {
            plan_datafusion_err!("No KQL statements were provided in the query string")
        })?)
    }
    
    async fn kql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        let mut provider = SessionContextProvider {
            state: self,
            tables: HashMap::with_capacity(10),
        };

        let catalog_list = self.catalog_list();
        for catalog in catalog_list.catalog_names() {
            let schema_list = catalog_list.catalog(&catalog).ok_or_else(|| DataFusionError::Plan(format!("Catalog '{catalog}' not found")))?;
            for schema in schema_list.schema_names() {
                let table_list = schema_list.schema(&schema).ok_or_else(|| DataFusionError::Plan(format!("Schema '{schema}' not found")))?;
                for table in table_list.table_names() {
                    let resolved_ref = ResolvedTableReference {
                        catalog: Arc::from(Box::from(catalog.clone())),
                        schema: Arc::from(Box::from(schema.clone())),
                        table: Arc::from(Box::from(table.clone()))
                    };
                    let table_provider = table_list.table(&table).await?.ok_or_else(|| DataFusionError::Plan(format!("Table '{table}' not found")))?;
                    provider.tables.insert(resolved_ref.to_string(), Arc::new(DefaultTableSource::new(table_provider)));
                    //println!("Table: {}", resolved_ref.to_string());
                }
            }
        }

        let kql = KqlToRel::new(&provider);
        match statement {
            Statement::TabularExpression(query) => kql.query_to_plan(&query),
            _ => not_impl_err!("Statement type not supported")
        }
    }
}

struct SessionContextProvider<'a> {
    state: &'a SessionState,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl<'a> ContextProvider for SessionContextProvider<'a> {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let catalog = &self.state.config_options().catalog;
        let name = name.resolve(&catalog.default_catalog, &catalog.default_schema).to_string();
        self.tables.get(&name).cloned().ok_or_else(|| DataFusionError::Plan(format!("Table '{}' not found", name)))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.udf(name).ok()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.udaf(name).ok()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }
    
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.udwf(name).ok()
    }
    
    fn options(&self) -> &ConfigOptions {
        !unimplemented!()
    }
    
    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }
    
    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }
    
    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}
