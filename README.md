# Rust-kql

The rust-kql project is a set of Rust crates for parsing and evaluating [Kusto Query Language (KQL)](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/) queries.
For evaluation, the [DataFusion](https://github.com/apache/arrow-datafusion) engine is used.
Due to differences between the Kusto engine and DataFusion, only a subset of KQL is supported.
Future work may include adding support for DataFusion features that are not present in the original KQL.

A simple command line tool is provided to demonstrate how to use the parser and planner crates.

## kqlparser
The `kqlparser` crate provides a parser for KQL queries. It is based on the `nom` parser library.
See the Status section below for the current state of the parser.
Most simple queries can be parsed, but due to unclearities in the KQL grammar, some queries may not be parsed.

## datafusion-kql
The `datafusion-kql` crate provides a planner to convert parsed KQL queries into DataFusion logical plans.
See the Status section below for the current state of the planner.
Due the differences between Kusto and DataFusion, the development of the planner is going slower than the parser.
Very simple queries can be executed, but uncertain if more complex queries will ever going to work.

## kq
The `kq` crate provides a simple command line tool to show how to use the `kqlparser` and `datafusion-kql` crates.

Example usage of the `kq` command line tool:
```shell
kq -f users.csv 'users | where name == "iwan" and age > 30'
kq -f logins.csv 'logins | summarize count(name) by name'
kq -f users.csv -f logins.csv 'logins | join (users) on name | project name, age, login_time'
```

## Status

✅ (mostly) done  
🚧 partial / in progress  
❌ not started

### Data Types

Type|Parser|Planner|
-|-|-|
bool|✅|✅
datetime|🚧|✅
decimal|🚧[^1]|❌
dynamic|✅|❌
guid|❌|❌
int|✅|✅
long|✅|✅
real|✅|✅
string|✅|✅
timespan|✅|✅

[^1]: Parsed as 64-bit floating number instead of 128-bit

### Operators

Operator|Parser|Planner|
-|-|-|
as|✅|❌
consume|✅|❌
count|✅|✅
datatable|✅|✅
distinct|✅|❌
evaluate|✅|❌
extend|✅|✅
externaldata|✅|❌
facet|✅|❌
find|✅|❌
fork|✅|❌
getschema|✅|✅
join|✅|🚧
lookup|✅|❌
mv-apply|✅|❌
mv-expand|✅|✅
print|✅|✅
project|✅|✅
project-away|✅|✅
project-keep|✅|✅
project-rename|✅|✅
project-reorder|✅|❌
parse|✅|❌
parse-where|✅|❌
parse-kv|✅|❌
partition|✅|❌
range|✅|❌
reduce|✅|❌
render|✅|❌
sample|✅|❌
sample-distinct|✅|❌
scan|❌|❌
search|❌|❌
serialize|✅|✅
summarize|✅|✅
sort|✅|✅
take|✅|✅
top|✅|✅
top-nested|❌|❌
top-hitters|❌|❌
union|✅|❌
where|✅|✅

### Statements
Type|Parser|Planner|
-|-|-|
alias|❌|❌
let|✅|❌
pattern|❌|❌
query parameters decleration|❌|❌
restrict|❌|❌
set|❌|❌
tabular expression|✅|🚧
