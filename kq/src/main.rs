use clap::Parser;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::execution::context::SessionContext;

use std::error::Error;
use std::ffi::OsStr;
use std::path::PathBuf;

mod kql;

use crate::kql::execute_kql;

#[derive(Parser)]
struct Cli {
    #[arg(short, long)]
    file: Vec<PathBuf>,
    query: Option<String>
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
        match file.extension().and_then(OsStr::to_str) {
            Some("arrow") => ctx.register_avro(base, file.as_os_str().to_str().unwrap(), Default::default()).await?,
            Some("avro") => ctx.register_avro(base, file.as_os_str().to_str().unwrap(), Default::default()).await?,
            Some("csv") => ctx.register_csv(base, file.as_os_str().to_str().unwrap(), Default::default()).await?,
            Some("json") => ctx.register_json(base, file.as_os_str().to_str().unwrap(), Default::default()).await?,
            Some("parquet") => ctx.register_parquet(base, file.as_os_str().to_str().unwrap(), Default::default()).await?,
            Some("kql") => {
                let query = std::fs::read_to_string(file)?;
                execute(&ctx, &query).await?
            },
            Some(ext) => return Err(format!("File extension '{}' not supported", ext).into()),
            None => return Err("File without extension not supported".into()),
        }
    }

    if let Some(query) = &args.query {
        execute(&ctx, query).await?;
        return Ok(());
    }
    Ok(())
}
