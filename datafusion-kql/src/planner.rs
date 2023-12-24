use arrow_schema::DataType;

use datafusion_common::TableReference;
use datafusion_common::{DataFusionError, Result};

use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::{AggregateUDF, Expr, Literal, ScalarUDF, TableSource};

use kqlparser::ast::{Expr as KqlExpr, Operator, TabularExpression, Value};

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
            KqlExpr::Value(v) => value_to_expr(v),
            KqlExpr::Ident(x) => col(x.as_str()),
            KqlExpr::Func(_, _) => return Err(DataFusionError::NotImplemented("Functions not implemented".to_string())),
        })
    }

    fn query_statement_to_plan(&self, ctx: &mut PlannerContext, query: TabularExpression) -> Result<LogicalPlan> {
        let source = self.ctx.get_table_provider(TableReference::from(query.name.as_str()))?;
        let mut builder = LogicalPlanBuilder::scan(query.name.clone(), source, None)?;

        for op in query.operators.into_iter() {
            builder = match op {
                Operator::Where(x) => builder.filter(self.ast_to_expr(ctx, &x)?)?,
                _ => return Err(DataFusionError::NotImplemented("Operator not implemented".to_string())),
            };
        }

        builder.build()
    }

    pub fn query_to_plan(&self, query: TabularExpression) -> Result<LogicalPlan> {
        self.query_statement_to_plan(&mut PlannerContext::default(), query)
    }
}

fn value_to_expr(val: &Value) -> Expr {
    match val {
        Value::Int(x) => x.lit(),
        Value::String(x) => x.lit(),
        Value::Bool(x) => x.lit(),
        _ => {
            panic!("Not supported")
        }
    }
}
