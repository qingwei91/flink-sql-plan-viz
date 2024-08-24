mod parse;

use std::string::String;
use crate::parse::{Operator, QueryPlan};
use parse::QueryPlans;
use wasm_bindgen::prelude::*;
use web_sys::HtmlTextAreaElement;
use yew::prelude::*;

fn render_query_plans(query_plans: &QueryPlans) -> Html {
    html! {
        <ul>
            { for query_plans.plans.iter().map(render_query_plan) }
        </ul>
    }
}

fn render_query_plan(plan: &QueryPlan) -> Html {
    html! {
        <li>
            <h2>{ format!("Section: {:?}", plan.section) }</h2>
            <ul>
                { for plan.operators.iter().map(render_operator) }
            </ul>
        </li>
    }
}

fn render_operator(operator: &Operator) -> Html {
    html! {
        <li>
            <h3>{ &operator.name }</h3>
            <ul>
                { for operator.attributes.iter().map(|(k, v)| html! {
                    <li>{ format!("{}: {:?}", k, v) }</li>
                }) }
            </ul>
            <ul>
                { for operator.children.iter().map(|child| render_operator(child)) }
            </ul>
        </li>
    }
}

#[function_component(App)]
fn app() -> Html {
    let query_plans = use_state(|| QueryPlans { plans: vec![] });
    let parse_err = use_state(String::new); // why Option never work
    let input_text = use_state(String::new);

    let oninput = {
        let input_text = input_text.clone();
        let query_plans = query_plans.clone();
        let parse_err = parse_err.clone();
        Callback::from(move |e: InputEvent| {
            let target = e.target().unwrap();
            let textarea = target.unchecked_into::<HtmlTextAreaElement>();
            let value = textarea.value();
            input_text.set(value.clone());

            // Parse the input and update QueryPlans
            match parse::parse_query_plan(&value) {
                Ok((_, parsed_plans)) => {
                    query_plans.set(parsed_plans);
                    parse_err.set(String::new());
                }
                Err(e) => {
                    parse_err.set(format!("Parsing error: {:?}", e).into());
                    // Handle parsing error (e.g., log it or show an error message)
                    // web_sys::console::log_1(&format!("Parsing error: {:?}", e).into());
                }
            }
        })
    };

    html! {
        <div style="display: flex;">
            <div style="width: 50%; padding: 10px;">
                <textarea
                    value={(*input_text).clone()}
                    oninput={oninput}
                    style="width: 100%; height: 500px;"
                    placeholder="Enter your query plan here..."
                />
            </div>
            <div style="width: 50%; padding: 10px; overflow-y: auto; height: 500px;">
                <h1>{"Query Plans Viewer"}</h1>
                { render_query_plans(&query_plans) }
                if !parse_err.is_empty() {
                    <div>{ format!("{}", (*parse_err).clone()) }</div>
                }
            </div>
        </div>
    }
}

fn main() -> () {
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
