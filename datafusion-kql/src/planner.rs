use arrow_schema::{DataType, TimeUnit};

use datafusion_common::{TableReference, JoinType, Column, DFSchema, DFField, ScalarValue};
use datafusion_common::{DataFusionError, Result};

use datafusion_expr::{aggregate_function, AggregateFunction as BuiltinAggregateFunction, BuiltinScalarFunction, BuiltInWindowFunction, WindowFrame, window_function, Values};
use datafusion_expr::expr::{AggregateFunction, Sort, ScalarFunction, WindowFunction};
use datafusion_expr::expr_fn::col;
use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::{Expr, Literal, TableSource};

use itertools::Itertools;

use kqlparser::ast::{Expr as KqlExpr, Operator, TabularExpression, Literal as KqlLiteral, Source, Type};

use std::collections::HashMap;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;

pub trait ContextProvider {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;
}

#[derive(Default, Clone)]
struct PlannerContext {}

pub struct KqlToRel<'a, S: ContextProvider> {
    ctx: &'a S,
}

impl<'a, S: ContextProvider> KqlToRel<'a, S> {
    pub fn new(ctx: &'a S) -> Self {
        KqlToRel { ctx }
    }

    fn func_to_expr(&self, ctx: &mut PlannerContext, name: &str, args: &Vec<KqlExpr>) -> Result<Expr> {
        let args = args.iter().map(|a| self.ast_to_expr(ctx, a)).collect::<Result<Vec<Expr>>>()?;
        Ok(match name {
            "make_list" => aggr_func(aggregate_function::AggregateFunction::ArrayAgg, args),
            "split" => scalar_func(BuiltinScalarFunction::StringToArray, args),
            "next" => window_func(BuiltInWindowFunction::Lag, args),
            "prev" => window_func(BuiltInWindowFunction::Lead, args),
            "row_number" => window_func(BuiltInWindowFunction::RowNumber, args),
            _ => if let Ok(f) = BuiltinScalarFunction::from_str(name) {
                scalar_func(f, args)
            } else if let Ok(f) = BuiltinAggregateFunction::from_str(name) {
                aggr_func(f, args)
            } else if let Ok(f) = BuiltInWindowFunction::from_str(name) {
                window_func(f, args)
            } else {
                return Err(DataFusionError::NotImplemented("Function not implemented".to_string()));
            }
        })
    }

    fn ast_to_expr(&self, ctx: &mut PlannerContext, ast: &KqlExpr) -> Result<Expr> {
        Ok(match ast {
            KqlExpr::Equals(x, y) => self.ast_to_expr(ctx, &x)?.eq(self.ast_to_expr(ctx, &y)?),
            KqlExpr::NotEquals(x, y) => self.ast_to_expr(ctx, &x)?.not_eq(self.ast_to_expr(ctx, &y)?),
            KqlExpr::And(x, y) => self.ast_to_expr(ctx, &x)?.and(self.ast_to_expr(ctx, &y)?),
            KqlExpr::Or(x, y) => self.ast_to_expr(ctx, &x)?.or(self.ast_to_expr(ctx, &y)?),
            KqlExpr::Add(x, y) => self.ast_to_expr(ctx, &x)? + self.ast_to_expr(ctx, &y)?,
            KqlExpr::Substract(x, y) => self.ast_to_expr(ctx, &x)? - self.ast_to_expr(ctx, &y)?,
            KqlExpr::Multiply(x, y) => self.ast_to_expr(ctx, &x)? * self.ast_to_expr(ctx, &y)?,
            KqlExpr::Divide(x, y) => self.ast_to_expr(ctx, &x)? / self.ast_to_expr(ctx, &y)?,
            KqlExpr::Modulo(x, y) => self.ast_to_expr(ctx, &x)? % self.ast_to_expr(ctx, &y)?,
            KqlExpr::Less(x, y) => self.ast_to_expr(ctx, &x)?.lt(self.ast_to_expr(ctx, &y)?),
            KqlExpr::Greater(x, y) => self.ast_to_expr(ctx, &x)?.gt(self.ast_to_expr(ctx, &y)?),
            KqlExpr::LessOrEqual(x, y) => self.ast_to_expr(ctx, &x)?.lt_eq(self.ast_to_expr(ctx, &y)?),
            KqlExpr::GreaterOrEqual(x, y) => self.ast_to_expr(ctx, &x)?.gt_eq(self.ast_to_expr(ctx, &y)?),
            KqlExpr::Literal(v) => literal_to_expr(v),
            KqlExpr::Ident(x) => col(x.as_str()),
            KqlExpr::Func(x, y) => self.func_to_expr(ctx, x.as_str(), y)?,
            _ => return Err(DataFusionError::NotImplemented("Expr not implemented".to_string()))
        })
    }

    fn query_statement_to_plan(&self, ctx: &mut PlannerContext, query: &TabularExpression) -> Result<LogicalPlan> {
        let mut builder = match &query.source {
            Source::Datatable(s, d) => LogicalPlanBuilder::from(LogicalPlan::Values(Values {
                schema: Arc::new(DFSchema::new_with_metadata(s.iter().map(|(n, t)| DFField::new::<TableReference>(None, n, type_to_datatype(t), true)).collect(), HashMap::default()).unwrap()),
                values: d.iter().chunks(s.len()).into_iter().map(|chunk| chunk.map(|r| self.ast_to_expr(ctx, r).unwrap()).collect()).collect()
            })),
            Source::Reference(n) => LogicalPlanBuilder::scan(n.clone(), self.ctx.get_table_provider(TableReference::from(n.as_str()))?, None)?,
            _ => return Err(DataFusionError::NotImplemented("Source not implemented".to_string())),
        };

        for op in query.operators.iter() {
            builder = match op {
                Operator::MvExpand(x) => builder.unnest_column(Column::from(x))?,
                Operator::Extend(x) => {
                    let current_schema = builder.schema().clone();
                    let current_columns = current_schema.fields().iter().map(|f| Expr::Column(f.qualified_column()));
                    builder.project(current_columns.chain(x.iter().map(|(a, b)| {
                        let mut expr = self.ast_to_expr(ctx, b).unwrap();
                        if let Some(alias) = a {
                            expr = expr.alias(alias);
                        }
                        expr
                    })))?
                },
                Operator::Join(_, x, y) => {
                    let keys: Vec<&str> = y.iter().map(|s| s.as_ref()).collect();
                    builder.join(self.query_statement_to_plan(ctx, &x)?, JoinType::Inner, (keys.clone(), keys), Option::None)?
                },
                Operator::Project(x) => builder.project(x.iter().map(|(a, b)| {
                    let mut expr = self.ast_to_expr(ctx, b).unwrap();
                    if let Some(alias) = a {
                        expr = expr.alias(alias);
                    }
                    expr
                }))?,
                Operator::Where(x) => builder.filter(self.ast_to_expr(ctx, &x)?)?,
                Operator::Serialize(x) => builder.window(x.iter().map(|(a, b)| {
                    let mut expr = self.ast_to_expr(ctx, b).unwrap();
                    if let Some(alias) = a {
                        expr = expr.alias(alias);
                    }
                    expr
                }))?,
                Operator::Summarize(x, y) => {
                    let mut ctx1 = ctx.clone();
                    builder.aggregate(y.iter().map(|z| self.ast_to_expr(&mut ctx1, z).unwrap()), x.iter().map(|z| self.ast_to_expr(ctx, z).unwrap()))?
                },
                Operator::Sort(o) => o.iter().fold(builder, |b, c| b.sort(iter::once(Expr::Sort(Sort::new(Box::new(col(c)), false, false)))).unwrap()),
                Operator::Take(x) => builder.limit(0, Some((*x).try_into().unwrap()))?,
                _ => return Err(DataFusionError::NotImplemented("Operator not implemented".to_string())),
            };
        }

        builder.build()
    }

    pub fn query_to_plan(&self, query: &TabularExpression) -> Result<LogicalPlan> {
        self.query_statement_to_plan(&mut PlannerContext::default(), query)
    }
}

fn type_to_datatype(t: &Type) -> DataType {
    match t {
        Type::Bool => DataType::Boolean,
        Type::Decimal => DataType::Float64,
        Type::Int => DataType::Int32,
        Type::Long => DataType::Int64,
        Type::Real => DataType::Float32,
        Type::String => DataType::Utf8,
        Type::Timespan => DataType::Duration(TimeUnit::Nanosecond),
        _ => panic!("Not supported")
    }
}

fn literal_to_expr(val: &KqlLiteral) -> Expr {
    match val {
        KqlLiteral::Bool(x) => ScalarValue::from(*x).lit(),
        KqlLiteral::Decimal(x) => ScalarValue::from(*x).lit(),
        KqlLiteral::Int(x) => ScalarValue::from(*x).lit(),
        KqlLiteral::Long(x) => ScalarValue::from(*x).lit(),
        KqlLiteral::Real(x) => ScalarValue::from(*x).lit(),
        KqlLiteral::String(x) => ScalarValue::from(x.clone()).lit(),
        KqlLiteral::Timespan(x) => ScalarValue::DurationNanosecond(*x).lit(),
        _ => panic!("Not supported")
    }
}

fn aggr_func(func: aggregate_function::AggregateFunction, args: Vec<Expr>) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(func, args, false, None, None))
}

fn scalar_func(func: BuiltinScalarFunction, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new(func, args))
}

fn window_func(func: BuiltInWindowFunction, args: Vec<Expr>) -> Expr {
    Expr::WindowFunction(WindowFunction::new(window_function::WindowFunction::BuiltInWindowFunction(func), args, vec![], vec![], WindowFrame::new(false)))
}
