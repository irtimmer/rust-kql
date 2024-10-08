use datafusion::arrow::datatypes::DataType;
use datafusion::catalog_common::ResolvedTableReference;
use datafusion::catalog_common::TableReference;
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::WindowUDF;
use datafusion::logical_expr::{TableSource, AggregateUDF, ScalarUDF, LogicalPlan};
use datafusion::logical_expr::planner::ContextProvider;

use datafusion_kql::planner::KqlToRel;

use kqlparser::ast::Statement;
use kqlparser::parser::parse;

use std::collections::HashMap;
use std::sync::Arc;

struct SessionContextProvider<'a> {
    state: &'a SessionState,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

pub async fn execute_kql(state: &SessionState, query: &str) -> Result<LogicalPlan, DataFusionError> {
    let mut provider = SessionContextProvider {
        state: &state,
        tables: HashMap::with_capacity(10),
    };

    let catalog_list = state.catalog_list();
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
            }
        }
    }

    let kql = KqlToRel::new(&provider);
    let query = parse(&query).unwrap().1;
    let mut plan = None;
    for stmt in &query {
        match stmt {
            Statement::TabularExpression(query) => {
                plan = Some(kql.query_to_plan(query)?);
            },
            _ => (),
        }
    }
    plan.ok_or(DataFusionError::Plan("No query found".to_string()))
}

impl<'a> ContextProvider for SessionContextProvider<'a> {
    fn get_table_source(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
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
