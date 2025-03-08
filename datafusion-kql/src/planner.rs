use arrow_schema::{DataType, Field, TimeUnit};

use datafusion_common::{TableReference, JoinType, Column, DFSchema, ScalarValue};
use datafusion_common::{DataFusionError, Result};

use datafusion_expr::{ExprSchemable, Values};
use datafusion_expr::expr::{AggregateFunction, ScalarFunction, WindowFunction};
use datafusion_expr::expr_fn::col;
use datafusion_expr::planner::ContextProvider;
use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::{Expr, Literal, SortExpr};

use itertools::Itertools;

use kqlparser::ast::{Expr as KqlExpr, Operator, TabularExpression, Literal as KqlLiteral, Source, Type};

use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use crate::LogicalPlanBuilderExt;

pub struct KqlToRel<'a, S: ContextProvider> {
    ctx: &'a S,
}

impl<'a, S: ContextProvider> KqlToRel<'a, S> {
    pub fn new(ctx: &'a S) -> Self {
        KqlToRel { ctx }
    }

    fn func_to_expr(&self, name: &str, args: &Vec<KqlExpr>) -> Result<Expr> {
        let args = args.iter().map(|a| self.ast_to_expr(a)).collect::<Result<Vec<Expr>>>()?;
        if let Some(f) = self.ctx.get_function_meta(&name) {
            Ok(Expr::ScalarFunction(ScalarFunction::new_udf(f, args)))
        } else if let Some(f) = self.ctx.get_aggregate_meta(&name) {
            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(f, args, false, None, None, None)))
        } else if let Some(f) = self.ctx.get_window_meta(&name) {
            Ok(Expr::WindowFunction(WindowFunction::new(f, args)))
        } else {
            return Err(DataFusionError::NotImplemented("Function not implemented".to_string()));
        }
    }

    fn ast_to_expr(&self, ast: &KqlExpr) -> Result<Expr> {
        Ok(match ast {
            KqlExpr::Equals(x, y) => self.ast_to_expr(&x)?.eq(self.ast_to_expr(&y)?),
            KqlExpr::NotEquals(x, y) => self.ast_to_expr(&x)?.not_eq(self.ast_to_expr(&y)?),
            KqlExpr::And(x, y) => self.ast_to_expr(&x)?.and(self.ast_to_expr(&y)?),
            KqlExpr::Or(x, y) => self.ast_to_expr(&x)?.or(self.ast_to_expr(&y)?),
            KqlExpr::Add(x, y) => self.ast_to_expr(&x)? + self.ast_to_expr(&y)?,
            KqlExpr::Substract(x, y) => self.ast_to_expr(&x)? - self.ast_to_expr(&y)?,
            KqlExpr::Multiply(x, y) => self.ast_to_expr(&x)? * self.ast_to_expr(&y)?,
            KqlExpr::Divide(x, y) => self.ast_to_expr(&x)? / self.ast_to_expr(&y)?,
            KqlExpr::Modulo(x, y) => self.ast_to_expr(&x)? % self.ast_to_expr(&y)?,
            KqlExpr::Less(x, y) => self.ast_to_expr(&x)?.lt(self.ast_to_expr(&y)?),
            KqlExpr::Greater(x, y) => self.ast_to_expr(&x)?.gt(self.ast_to_expr(&y)?),
            KqlExpr::LessOrEqual(x, y) => self.ast_to_expr(&x)?.lt_eq(self.ast_to_expr(&y)?),
            KqlExpr::GreaterOrEqual(x, y) => self.ast_to_expr(&x)?.gt_eq(self.ast_to_expr(&y)?),
            KqlExpr::Literal(v) => literal_to_expr(v),
            KqlExpr::Ident(x) => col(x.as_str()),
            KqlExpr::Func(x, y) => self.func_to_expr(x.as_str(), y)?,
            _ => return Err(DataFusionError::NotImplemented("Expr not implemented".to_string()))
        })
    }

    fn query_statement_to_plan(&self, query: &TabularExpression) -> Result<LogicalPlan> {
        let mut builder = match &query.source {
            Source::Print(v) => {
                let values = v.iter()
                    .map(|(_, v)| self.ast_to_expr(v))
                    .collect::<Result<Vec<Expr>>>()?;

                let mut print_idx = 0;
                let schema = DFSchema::empty();
                let fields = values.iter()
                    .zip(v)
                    .map(|(v, (n, _))| {
                        let name = n.clone().unwrap_or_else(|| {
                            let name = format!("print_{}", print_idx);
                            print_idx += 1;
                            name
                        });
                        Ok((None, Arc::new(Field::new(name, v.get_type(&schema)?, true))))
                    })
                    .collect::<Result<Vec<(Option<TableReference>, Arc<Field>)>>>()?;

                LogicalPlanBuilder::from(LogicalPlan::Values(Values {
                    schema: Arc::new(DFSchema::new_with_metadata(fields, HashMap::default())?),
                    values: vec![values]
                }))
            }
            Source::Datatable(s, d) => LogicalPlanBuilder::from(LogicalPlan::Values(Values {
                schema: Arc::new(DFSchema::new_with_metadata(s.iter().map(|(n, t)| (None::<TableReference>, Arc::new(Field::new(n, type_to_datatype(t), true)))).collect(), HashMap::default()).unwrap()),
                values: d.iter().chunks(s.len()).into_iter().map(|chunk| chunk.map(|r| self.ast_to_expr(r).unwrap()).collect()).collect()
            })),
            Source::Reference(n) => LogicalPlanBuilder::scan(n.clone(), self.ctx.get_table_source(TableReference::from(n.as_str()))?, None)?,
            _ => return Err(DataFusionError::NotImplemented("Source not implemented".to_string())),
        };

        for op in query.operators.iter() {
            builder = match op {
                Operator::MvExpand(x) => builder.mv_expand(Column::from(x))?,
                Operator::Extend(x) => builder.extend(x.iter().map(|(a, b)| (a.clone(), self.ast_to_expr(b).unwrap())))?,
                Operator::Join(_, x, y) => {
                    let keys: Vec<&str> = y.iter().map(|s| s.as_ref()).collect();
                    builder.join(self.query_statement_to_plan(&x)?, JoinType::Inner, (keys.clone(), keys), Option::None)?
                },
                Operator::Project(x) => builder.project_with_alias(x.iter().map(|(a, b)| (a.clone(), self.ast_to_expr(b).unwrap())))?,
                Operator::ProjectAway(x) => builder.project_away(x)?,
                Operator::ProjectKeep(x) => builder.project_keep(x)?,
                Operator::ProjectRename(x) => builder.project_rename(x.iter().cloned().collect())?,
                Operator::Where(x) => builder.filter(self.ast_to_expr(&x)?)?,
                Operator::Serialize(x) => builder.serialize(x.iter().map(|(a, e)| (a.clone(), self.ast_to_expr(e).unwrap())))?,
                Operator::Summarize(x, y) => builder.summarize(x.iter().map(|(a, b)| (a.clone(), self.ast_to_expr(b).unwrap())), y.iter().map(|x| self.ast_to_expr(x).unwrap()))?,
                Operator::Sort(o) => builder.sort(o.iter().map(|c| SortExpr::new(col(c), false, false)))?,
                Operator::Take(x) => builder.take(*x)?,
                _ => return Err(DataFusionError::NotImplemented("Operator not implemented".to_string())),
            };
        }

        builder.build()
    }

    pub fn query_to_plan(&self, query: &TabularExpression) -> Result<LogicalPlan> {
        self.query_statement_to_plan(query)
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
