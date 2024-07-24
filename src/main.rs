use std::collections::HashMap;
use std::ops::Deref;
use nom::{
    IResult,
    bytes::complete::{tag, take_until},
    character::complete::{alphanumeric1, multispace0},
    sequence::delimited,
    error::{VerboseError, VerboseErrorKind}
};
use nom::branch::alt;
use nom::bytes::complete::{take_until1, take_while1};
use nom::character::complete::{alpha1, anychar, char, line_ending, newline};
use nom::combinator::{map, opt};
use nom::multi::{many0, many1, separated_list0};
use nom::sequence::{preceded, separated_pair};
use warp::Filter;

type Res<T, U> = IResult<T, U, VerboseError<T>>;

#[derive(Debug, PartialEq)]
enum Section {
    AbstractSyntaxTree,
    OptimizedPhysicalPlan,
    OptimizedExecutionPlan,
}

fn parse_section(input: &str) -> Res<&str, Section> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("==")(input)?;
    let (input, content) = delimited(
        multispace0,
        take_until("=="),
        preceded(tag("=="), multispace0)
    )(input)?;

    let section = match content.trim() {
        "Abstract Syntax Tree" => Section::AbstractSyntaxTree,
        "Optimized Physical Plan" => Section::OptimizedPhysicalPlan,
        "Optimized Execution Plan" => Section::OptimizedExecutionPlan,
        _ => return Err(nom::Err::Error(VerboseError { errors: vec![
            (content, VerboseErrorKind::Context("Section not found")),
        ] })),
    };

    Ok((input, section))
}

#[derive(Debug, PartialEq)]
struct Operator {
    name: String,
    attributes: HashMap<String, String>,
    children: Vec<Box<Operator>>
}

#[derive(Debug, PartialEq)]
struct QueryPlan {
    section: Section,
    operators: Vec<Operator>
}

#[derive(Debug, PartialEq)]
struct QueryPlans {
    plans: Vec<QueryPlan>
}

fn parse_identifier(input: &str) -> Res<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

fn parse_value(input: &str) -> Res<&str, &str> {
    take_while1(|c|c != ',' && c !=')')(input)
}

fn parse_attribute(input: &str) -> Res<&str, (String, String)> {
    map(
        separated_pair(parse_identifier, tag("="), parse_expression),
        |(k, v)| (k.to_string(), v.trim().to_string())
    )(input)
}

fn parse_operator_line(input: &str) -> Res<&str, (String, HashMap<String, String>)> {
    let (input, name) = alpha1(input)?;
    let (input, attributes) = delimited(
        char('('),
        separated_list0(tag(", "), parse_attribute),
        char(')')
    )(input)?;

    Ok((input, (name.to_string(), attributes.into_iter().collect())))
}

fn parse_child_operator(input: &str) -> Res<&str, Operator> {
    let (input, _) =multispace0(input)?;
    delimited(
        alt((tag(":- "), tag("+- "))),
        parse_operator,
        multispace0
    )(input)
}

fn parse_operator(input: &str) -> Res<&str, Operator> {
    let (input, (name, attributes)) = parse_operator_line(input)?;
    let (input, _) = opt(line_ending)(input)?;
    let (input, children) = many0(parse_child_operator)(input)?;
    let boxed_children = children.into_iter().map(|o|Box::new(o)).collect();

    Ok((input, Operator {
        name,
        attributes,
        children: boxed_children,
    }))
}

fn parse_expression(input: &str) -> Res<&str, &str> {
    delimited(tag("["), take_until1("]") , tag("]"))(input)
}

fn parse_full_section(input: &str) -> Res<&str, QueryPlan> {
    let (input, section) = parse_section(input)?;
    let (input, operators) = many1(parse_operator)(input)?;
    Ok((input, QueryPlan{section, operators}))
}

fn parse_query_plan(input: &str) -> Res<&str, QueryPlans> {
    let (input, _) = opt(line_ending)(input)?;
    let (input, plans) = many1(parse_full_section)(input)?;
    let (input, _) = opt(line_ending)(input)?;
    Ok((input, QueryPlans {plans}))
}


fn main() {
    let input = "
== Abstract Syntax Tree ==
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
    let parse_result = parse_query_plan("
== Abstract Syntax Tree ==
LogicalProject(account_id=[$0], log_ts=[$1], amount=[$2], account_id0=[$3], amount0=[$4], transaction_time=[$5])
+- LogicalFilter(condition=[>($4, 1000)])
   +- LogicalJoin(condition=[=($0, $3)], joinType=[inner])
      :- LogicalTableScan(table=[[default_catalog, default_database, spend_report]])
      +- LogicalWatermarkAssigner(rowtime=[transaction_time], watermark=[-($2, 5000:INTERVAL SECOND)])
         +- LogicalTableScan(table=[[default_catalog, default_database, transactions]])"
    );

    match parse_result {
        Ok(mut pairs) => {
            println!("{:#?}", pairs.1);
        }
        Err(e) => {
            eprintln!("Parse error: {}", e);
        }
    }
}