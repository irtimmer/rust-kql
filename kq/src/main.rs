use clap::Parser;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::CsvReadOptions;

use std::error::Error;
use std::path::PathBuf;

mod kql;

use crate::kql::execute_kql;

#[derive(Parser)]
struct Cli {
    #[arg(short, long)]
    file: Vec<PathBuf>,
    query: String
}

async fn execute(ctx: &SessionContext, query: &str) -> Result<(), Box<dyn Error>> {
    let state = ctx.state();
    let plan = execute_kql(&state, query).await?;
    let results: Vec<RecordBatch> = ctx.execute_logical_plan(plan).await?.collect().await?;
    pretty::print_batches(&results)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();

    let ctx = SessionContext::new();
    for file in &args.file {
        let base = file.file_stem().unwrap().to_str().unwrap();
        ctx.register_csv(base, file.as_os_str().to_str().unwrap(), CsvReadOptions::default()).await?;
    }

    execute(&ctx, &args.query).await?;
    Ok(())
}
