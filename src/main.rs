mod parse;

use crate::parse::{Operator, QueryPlan};

use itertools::Itertools;
use parse::QueryPlans;
use std::string::String;
use wasm_bindgen::prelude::*;
use web_sys::HtmlTextAreaElement;
use yew::prelude::*;

fn render_query_plans(query_plans: &QueryPlans) -> Html {
    html! {
        <div>
            { for query_plans.plans.iter().map(render_query_plan) }
        </div>
    }
}

fn render_query_plan(plan: &QueryPlan) -> Html {
    html! {
        <Collapsible title={format!("Section: {:?}", plan.section)} class="query-plan">
            { for plan.operators.iter().map(render_operator) }
        </Collapsible>
    }
}

fn render_operator(operator: &Operator) -> Html {
    html! {
        <Collapsible title={operator.name.clone()} class="operator">
            <div class="attributes">
                { for operator.attributes.iter().sorted_by_key(|x|x.0)
                    .enumerate()
                    .map(|(idx, (k, v))| html! {
                    <span>{
                        if idx == 0 {
                            format!("{}=[{}]", k, v)
                        } else {
                            format!(", {}=[{}]", k, v)
                        }
                    }</span>
                }) }
            </div>
            { for operator.children.iter().map(|bo|render_operator(bo)) }
        </Collapsible>
    }
}

#[derive(Properties, PartialEq)]
pub struct CollapsibleProps {
    pub title: String,
    pub children: Children,
    #[prop_or_default]
    pub class: Classes,
}

#[function_component(Collapsible)]
pub fn collapsible(props: &CollapsibleProps) -> Html {
    let is_collapsed = use_state(|| false);

    let toggle_collapse = {
        let is_collapsed = is_collapsed.clone();
        Callback::from(move |_| is_collapsed.set(!*is_collapsed))
    };

    html! {
        <div class={classes!("collapsible", props.class.clone())}>
            <div class="collapsible-header" onclick={toggle_collapse}>
                <span class="collapse-icon">
                    {if *is_collapsed { "▶ " } else { "▼ " }}
                </span>
                <span class="collapsible-title">{&props.title}</span>
            </div>
            if !*is_collapsed {
                <div class="collapsible-content">
                    { for props.children.iter() }
                </div>
            }
        </div>
    }
}

#[function_component(App)]
fn app() -> Html {
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

    let (_, init_plan) = parse::parse_query_plan(input).unwrap();

    let query_plans = use_state(|| init_plan);
    let parse_err = use_state(String::new); // why Option never work
    let input_text = use_state(|| input.to_string());

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
            <div style="width: 50%; max-width: 100%; padding: 10px;">
                <textarea
                    value={(*input_text).clone()}
                    oninput={oninput}
                    style="width: 100%; height: 90vh; position: sticky; top: 20px;"
                    placeholder="Enter your query plan here..."
                />
            </div>
            <div style="padding: 10px; overflow-y: auto; height: 90%;">
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

    yew::Renderer::<App>::new().render();
}
