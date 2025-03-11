use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, TimeUnit};

use datafusion_common::{Column, DFSchema, Result, ScalarValue};

use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, SortExpr, Values};

use datafusion_functions_aggregate::count::count_all;

use wildmatch::WildMatch;

pub trait LogicalPlanBuilderExt {
    fn count(self) -> Result<LogicalPlanBuilder>;
    fn extend<I: IntoIterator<Item = (Option<impl Into<String>>, Expr)>>(self, columns: I) -> Result<LogicalPlanBuilder>;
    fn getschema(self) -> Result<LogicalPlanBuilder>;
    fn project_away<I: IntoIterator<Item = impl AsRef<str>>>(self, columns: I) -> Result<LogicalPlanBuilder>;
    fn project_keep<I: IntoIterator<Item = impl AsRef<str>>>(self, columns: I) -> Result<LogicalPlanBuilder>;
    fn project_rename(self, columns: HashMap<String, String>) -> Result<LogicalPlanBuilder>;
    fn project_with_alias<I: IntoIterator<Item = (Option<impl Into<String>>, Expr)>>(self, columns: I) -> Result<LogicalPlanBuilder>;
    fn mv_expand(self, column: impl Into<Column>) -> Result<LogicalPlanBuilder>;
    fn serialize<I: IntoIterator<Item = (Option<impl Into<String>>, Expr)>>(self, columns: I) -> Result<LogicalPlanBuilder>;
    fn summarize<G: IntoIterator<Item = (Option<impl Into<String>>, Expr)>, A: IntoIterator<Item = Expr>>(self, group: G, aggr: A) -> Result<LogicalPlanBuilder>;
    fn take(self, count: u32) -> Result<LogicalPlanBuilder>;
    fn top(self, count: u32, expr: impl Into<Expr>, asc: bool, nulls_first: bool) -> Result<LogicalPlanBuilder>;
}

impl LogicalPlanBuilderExt for LogicalPlanBuilder {
    fn count(self) -> Result<LogicalPlanBuilder> {
        self.aggregate(Vec::<Expr>::new(), vec![count_all().alias("count")])
    }

    fn extend<I: IntoIterator<Item = (Option<impl Into<String>>, Expr)>>(self, columns: I) -> Result<Self> {
        let current_schema = self.schema().clone();
        let current_columns = current_schema.columns().into_iter().map(|f| Expr::Column(f));
        self.project(current_columns.chain(alias_columns(columns)))
    }

    fn getschema(self) -> Result<LogicalPlanBuilder> {
        let schema = Arc::new(DFSchema::from_unqualified_fields(Fields::from(vec![
            Field::new("ColumnName", DataType::Utf8, false),
            Field::new("ColumnOrdinal", DataType::Int64, false),
            Field::new("DataType", DataType::Utf8, false),
            Field::new("ColumnType", DataType::Utf8, false)
        ]), HashMap::default())?);
        let values = self.schema().fields().iter().enumerate().map(|(i, f)| {
            vec![
                Expr::Literal(ScalarValue::Utf8(Some(f.name().to_string()))),
                Expr::Literal(ScalarValue::Int64(Some(i as i64))),
                Expr::Literal(ScalarValue::Utf8(Some(f.data_type().to_string()))),
                Expr::Literal(ScalarValue::Utf8(Some(datatype_to_string(f.data_type()).to_string())))
            ]
        }).collect();

        Ok(LogicalPlanBuilder::from(LogicalPlan::Values(Values {
            schema,
            values
        })))
    }

    fn project_away<I: IntoIterator<Item = impl AsRef<str>>>(self, columns: I) -> Result<LogicalPlanBuilder> {
        let wildcards: Vec<WildMatch> = columns.into_iter().map(|w| WildMatch::new(w.as_ref())).collect();
        let current_schema = self.schema().clone();
        let columns = current_schema.columns().into_iter()
            .filter(|f| wildcards.iter().all(|w| !w.matches(f.name())))
            .map(|f| Expr::Column(f));

        self.project(columns)
    }

    fn project_keep<I: IntoIterator<Item = impl AsRef<str>>>(self, columns: I) -> Result<LogicalPlanBuilder> {
        let wildcards: Vec<WildMatch> = columns.into_iter().map(|w| WildMatch::new(w.as_ref())).collect();
        let current_schema = self.schema().clone();
        let columns = current_schema.columns().into_iter()
            .filter(|f| wildcards.iter().any(|w| w.matches(f.name())))
            .map(|f| Expr::Column(f));

        self.project(columns)
    }

    fn project_rename(self, columns: HashMap<String, String>) -> Result<Self> {
        let current_schema = self.schema().clone();
        let columns = current_schema.columns().into_iter()
            .map(|f| {
                let name = columns.get(f.name()).cloned().unwrap_or_else(|| f.name().to_string());
                Expr::Column(f).alias(name)
            });

        self.project(columns)
    }

    fn project_with_alias<I: IntoIterator<Item = (Option<impl Into<String>>, Expr)>>(self, columns: I) -> Result<Self> {
        self.project(alias_columns(columns))
    }

    fn mv_expand(self, column: impl Into<Column>) -> Result<Self> {
        self.unnest_column(column.into())
    }

    fn serialize<I: IntoIterator<Item = (Option<impl Into<String>>, Expr)>>(self, columns: I) -> Result<Self> {
        self.window(alias_columns(columns))
    }

    fn summarize<G: IntoIterator<Item = (Option<impl Into<String>>, Expr)>, A: IntoIterator<Item = Expr>>(self, group: G, aggr: A) -> Result<Self> {
        self.aggregate(alias_columns(group), aggr)
    }

    fn take(self, count: u32) -> Result<Self> {
        self.limit(0, Some(count.try_into().unwrap()))
    }

    fn top(self, count: u32, expr: impl Into<Expr>, asc: bool, nulls_first: bool) -> Result<Self> {
        self.sort(vec![SortExpr {
            expr: expr.into(),
            asc,
            nulls_first
        }])?.limit(0, Some(count.try_into().unwrap()))
    }
}

/// Helper function to convert a collection of optional name-expression pairs
/// into expressions with aliases where names are provided
fn alias_columns<I, S>(columns: I) -> impl Iterator<Item = Expr>
where
    I: IntoIterator<Item = (Option<S>, Expr)>,
    S: Into<String>,
{
    columns.into_iter().map(|(alias, expr)| {
        if let Some(alias) = alias {
            expr.alias(alias)
        } else {
            expr
        }
    })
}

fn datatype_to_string(t: &DataType) -> &str {
    match t {
        DataType::Boolean => "bool",
        DataType::Duration(TimeUnit::Nanosecond) => "timespan",
        DataType::Float32 => "real",
        DataType::Float64 => "decimal",
        DataType::Int32 => "int",
        DataType::Int64 => "long",
        DataType::Utf8 => "string",
        _ => "unknown"
    }
}
