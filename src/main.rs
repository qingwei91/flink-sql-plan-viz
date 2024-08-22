mod parse;


fn main() -> () {
    let ast = "== Abstract Syntax Tree ==
LogicalProject(account_id=[$0], log_ts=[$1], amount=[$2], account_id0=[$3], amount0=[$4], transaction_time=[$5])
+- LogicalFilter(condition=[>($4, 1000)])
   +- LogicalJoin(condition=[=($0, $3)], joinType=[inner])
      :- LogicalTableScan(table=[[default_catalog, default_database, spend_report]])
      +- LogicalWatermarkAssigner(rowtime=[transaction_time], watermark=[-($2, 5000:INTERVAL SECOND)])
         +- LogicalTableScan(table=[[default_catalog, default_database, transactions]])
";
    let input = "== Abstract Syntax Tree ==
LogicalProject(account_id=[$0], log_ts=[$1], amount=[$2], account_id0=[$3], amount0=[$4], transaction_time=[$5])
+- LogicalFilter(condition=[>($4, 1000)])
   +- LogicalJoin(condition=[=($0, $3)], joinType=[inner])
      :- LogicalTableScan(table=[[default_catalog, default_database, spend_report]])
      +- LogicalWatermarkAssigner(rowtime=[transaction_time], watermark=[-($2, 5000:INTERVAL SECOND)])
         +- LogicalTableScan(table=[[default_catalog, default_database, transactions]])
== Optimized Physical Plan ==
Join(joinType=[InnerJoin], where=[=(account_id, account_id0)], select=[account_id, log_ts, amount, account_id0, amount0, transaction_time], leftInputSpec=[HasUniqueKey], rightInputSpec=[NoUniqueKey])
:- Exchange(distribution=[hash[account_id]])
:  +- TableSourceScan(table=[[default_catalog, default_database, spend_report]], fields=[account_id, log_ts, amount])
+- Exchange(distribution=[hash[account_id]])
   +- Calc(select=[account_id, amount, CAST(transaction_time AS TIMESTAMP(3)) AS transaction_time], where=[>(amount, 1000)])
      +- TableSourceScan(table=[[default_catalog, default_database, transactions, watermark=[-(transaction_time, 5000:INTERVAL SECOND)]]], fields=[account_id, amount, transaction_time])

== Optimized Execution Plan ==
Join(joinType=[InnerJoin], where=[(account_id = account_id0)], select=[account_id, log_ts, amount, account_id0, amount0, transaction_time], leftInputSpec=[HasUniqueKey], rightInputSpec=[NoUniqueKey])
:- Exchange(distribution=[hash[account_id]])
:  +- TableSourceScan(table=[[default_catalog, default_database, spend_report]], fields=[account_id, log_ts, amount])
+- Exchange(distribution=[hash[account_id]])
   +- Calc(select=[account_id, amount, CAST(transaction_time AS TIMESTAMP(3)) AS transaction_time], where=[(amount > 1000)])
      +- TableSourceScan(table=[[default_catalog, default_database, transactions, watermark=[-(transaction_time, 5000:INTERVAL SECOND)]]], fields=[account_id, amount, transaction_time])
";

    // let parse_result = parse_query_plan(input);
    let parse_result = parse::parse_query_plan(input);

    match parse_result {
        Ok(pairs) => {
            println!("{:#?}", pairs.1);
        }
        Err(e) => {
            eprintln!("Parse error: {}", e);
        }
    }
}
