use std::collections::HashMap;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_until, take_while, take_while1};
use nom::character::complete::{alpha1, char, digit1, line_ending, multispace0, space0};
use nom::combinator::{map, opt, peek};
use nom::error::{VerboseError, VerboseErrorKind};
use nom::IResult;
use nom::multi::{many0, many1, separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};

type Res<T, U> = IResult<T, U, VerboseError<T>>;

#[derive(Debug, PartialEq)]
pub enum Section {
    AbstractSyntaxTree,
    OptimizedPhysicalPlan,
    OptimizedExecutionPlan,
}
#[derive(Debug, PartialEq)]
pub enum Expression {
    Binding {
        name: String,
    },
    Projection {
        fields: Vec<Box<Expression>>,
    },
    FQName {
        name: Vec<String>,
    },
    BiOp {
        op: String,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    TypedLiteral {
        value: i16,
        _type: String,
    },
    NumLiteral(i16),
    HashDistribution(String),
    CastExpression {
        input: String,
        cast_type: String,
    },
    Unknown(String),
}

#[derive(Debug, PartialEq)]
pub struct Operator {
    name: String,
    attributes: HashMap<String, Expression>,
    children: Vec<Box<Operator>>,
}

#[derive(Debug, PartialEq)]
pub struct QueryPlan {
    section: Section,
    operators: Vec<Operator>,
}

#[derive(Debug, PartialEq)]
pub struct QueryPlans {
    plans: Vec<QueryPlan>,
}
fn parse_identifier(input: &str) -> Res<&str, &str> {
    let (input, id) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    Ok((input, id))
}

fn parse_attribute(input: &str) -> Res<&str, (String, Expression)> {
    let (input, (k, v)) = separated_pair(
        parse_identifier,
        tag("="),
        delimited(tag("["), parse_expression, tag("]")),
    )(input)?;
    Ok((input, (k.to_string(), v)))
}

fn parse_operator_line(input: &str) -> Res<&str, (String, HashMap<String, Expression>)> {
    let (input, name) = alpha1(input)?;
    let (input, attributes) = delimited(
        char('('),
        separated_list0(tag(", "), parse_attribute),
        tag(")\n"),
    )(input)?;

    Ok((input, (name.to_string(), attributes.into_iter().collect())))
}

fn parse_until_unbalanced_bracket(input: &str) -> Res<&str, Expression> {
    let mut bracket_count = 0;

    for (i, c) in input.char_indices() {
        match c {
            '[' => bracket_count += 1,
            ']' => {
                if bracket_count == 0 {
                    return Ok((&input[i..], Expression::Unknown(input[..i].to_string())));
                }
                bracket_count -= 1;
            }
            _ => {}
        }
    }

    Ok(("", Expression::Unknown(input.to_string())))
}
fn parse_fqname() -> impl Fn(&str) -> Res<&str, Expression> {
    move |input| {
        map(
            delimited(
                tag("["),
                separated_list0(tuple((space0, tag(","), space0)), parse_identifier),
                tag("]"),
            ),
            |parts| Expression::FQName {
                name: parts.iter().map(|s| s.to_string()).collect(),
            },
        )(input)
    }
}

fn parse_type(input: &str) -> Res<&str, String> {
    let (input, type_name) = parse_identifier(input)?;
    let (input, parameters) = opt(delimited(
        char('('),
        take_while1(|c: char| c.is_numeric() || c == ','),
        char(')'),
    ))(input)?;

    let full_type = match parameters {
        Some(params) => format!("{}({})", type_name, params),
        None => type_name.to_string(),
    };

    Ok((input, full_type))
}

fn parse_cast_expression(input: &str) -> Res<&str, Expression> {
    let (input, _) = delimited(tag("CAST"), multispace0, tag("("))(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = delimited(multispace0, tag("AS"), multispace0)(input)?;
    let (input, tpe) = terminated(parse_type, pair(multispace0, tag(")")))(input)?;

    Ok((input, Expression::CastExpression {input: name.to_string(), cast_type: tpe}))
}


fn parse_hash_dist(input: &str) -> Res<&str, Expression> {
    // a hacky way to express at least 2 elements, sep by ,
    // this is needed to distinguish with single element expression
    let (input, id) =
        preceded(tag("hash"), delimited(tag("["), parse_identifier, tag("]")))(input)?;
    let dist = Expression::HashDistribution(id.to_string());
    Ok((input, dist))
}
fn parse_projection(input: &str) -> Res<&str, Expression> {

    let parse_binding = map(parse_identifier, |c| Expression::Binding {name: c.to_string()});
    let parse_binding2 = map(parse_identifier, |c| Expression::Binding {name: c.to_string()});
    let projection_exp = alt((parse_cast_expression, parse_type_lit(), parse_binding, parse_number));
    let projection_exp2 = alt((parse_cast_expression, parse_type_lit(), parse_binding2, parse_number));
    // a hacky way to express at least 2 elements, sep by ,
    // this is needed to distinguish with single element expression
    let (input, (head, tail)) = separated_pair(
        projection_exp,
        tag(", "),
        terminated(separated_list1(tag(", "), projection_exp2), peek(tag("]"))),
    )(input)?;
    let mut ids = vec![head];
    ids.extend(tail);

    // let (input, ids) = terminated(separated_list0(tag(", "), parse_identifier), peek(tag("]")))?;
    let proj = Expression::Projection {
        fields: ids.into_iter().map(|s| Box::new(s)).collect(),
    };
    Ok((input, proj))
}


fn parse_bi_op() -> impl Fn(&str) -> Res<&str, Expression> {
    move |input| {
        let (input, op) =
            alt((tag("="), tag("<"), tag(">"), tag("-"), tag("<="), tag(">=")))(input)?;

        let (input, args) = delimited(
            tag("("),
            separated_pair(
                parse_expression,
                tuple((space0, tag(","), space0)),
                parse_expression,
            ),
            tag(")"),
        )(input)?;

        Ok((
            input,
            Expression::BiOp {
                op: op.to_string(),
                left: Box::new(args.0),
                right: Box::new(args.1),
            },
        ))
    }
}

fn parse_type_lit() -> impl Fn(&str) -> Res<&str, Expression> {
    |input| {
        map(separated_pair(digit1, tag(":"), take_until(")")), |pair| {
            Expression::TypedLiteral {
                value: str::parse(pair.0).unwrap(),
                _type: pair.1.to_string(),
            }
        })(input)
    }
}

fn parse_number(input: &str) -> Res<&str, Expression> {
    let (input, num_str) = take_while1(|c: char| c.is_numeric())(input)?;
    Ok((input, Expression::NumLiteral(num_str.parse().unwrap())))
}


fn parse_binding() -> impl Fn(&str) -> Res<&str, Expression> {
    move |input| {
        map(pair(opt(tag("$")), parse_identifier), |p| {
            Expression::Binding {
                name: p.0.unwrap_or("").to_string() + p.1,
            }
        })(input)
    }
}fn parse_expression(input: &str) -> Res<&str, Expression> {
    // order matters! as these are all greedy consumption and will happily return whatever they can parse
    let (i, r) = alt((
        parse_until_unbalanced_bracket,
        parse_fqname(),
        parse_cast_expression,
        parse_hash_dist,
        parse_bi_op(),
        parse_type_lit(),
        parse_projection,
        parse_binding(),
        parse_number, // not a good way, we should have some sort of tokenization
    ))(input)?;
    Ok((i, r))
}

fn parse_section(input: &str) -> Res<&str, Section> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("==")(input)?;
    let (input, content) = delimited(
        multispace0,
        take_until("=="),
        preceded(tag("=="), multispace0),
    )(input)?;

    let section = match content.trim() {
        "Abstract Syntax Tree" => Section::AbstractSyntaxTree,
        "Optimized Physical Plan" => Section::OptimizedPhysicalPlan,
        "Optimized Execution Plan" => Section::OptimizedExecutionPlan,
        _ => {
            return Err(nom::Err::Error(VerboseError {
                errors: vec![(content, VerboseErrorKind::Context("Section not found"))],
            }))
        }
    };

    Ok((input, section))
}

fn parse_operator_ident(input: &str) -> Res<&str, usize> {
    let (input, indent) = take_while(|c: char| !c.is_alphanumeric())(input)?;
    Ok((input, indent.len()))
}

fn parse_operator_(input: &str) -> Res<&str, Operator> {
    /*
    This need to grab a line, parse into operator
    Get its indentation
    Peek next line of identation
    if next line ident is larger than current line, parse that and stick into children
    then do thesame for next line
    */
    let (input, indent) = parse_operator_ident(input)?;
    let (input, (name, attributes)) = parse_operator_line(input)?;
    let (input, next_indent) = peek(parse_operator_ident)(input)?;
    let (input, children) = if next_indent > indent {
        many0(parse_operator_)(input)?
    } else {
        Ok((input, Vec::new()))?
    };
    Ok((
        input,
        Operator {
            name,
            attributes,
            children: children.into_iter().map(|o| Box::new(o)).collect(),
        },
    ))
}

fn parse_full_section(input: &str) -> Res<&str, QueryPlan> {
    let (input, section) = parse_section(input)?;
    // let (input, opStr) = take_until("== ")(input)?;
    // let (input, operators) = terminated(many1(parse_operator), take_until1("== "))(input)?;
    // todo: maybe we should detect indentation
    let (input, operators) = many0(parse_operator_)(input)?;
    Ok((input, QueryPlan { section, operators }))
}

pub fn parse_query_plan(input: &str) -> Res<&str, QueryPlans> {
    let (input, _) = opt(line_ending)(input)?;
    let (input, plans) = many1(parse_full_section)(input)?;
    let (input, _) = opt(line_ending)(input)?;
    Ok((input, QueryPlans { plans }))
}