// Copyright 2024 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod process_monitor;

use std::sync::Arc;

use drasi_core::{
    models::{Element, ElementMetadata, ElementPropertyMap, ElementReference, SourceChange},
    query::QueryBuilder,
};
use drasi_query_ast::ast::{
    Annotation, BinaryExpression, Direction, Expression, Literal, MatchClause, NodeMatch,
    ProjectionClause, Query, QueryPart, RelationMatch, UnaryExpression,
};
use serde_json::json;



#[allow(clippy::print_stdout, clippy::unwrap_used)]
#[tokio::main]
async fn main() {
    let query_str = "
   MATCH (v:Vehicle)
     WHERE v.miles > 60000
    RETURN v.color";

    // Use hardcoded AST instead of parsing
    let hardcoded_ast = create_hardcoded_ast();

     let query_builder = QueryBuilder::new(query_str).with_custom_ast(hardcoded_ast);
    //let query_builder = QueryBuilder::new(query_str).with_query_parser(QueryParserType::GQL);
    //let query_builder = QueryBuilder::new(query_str);
    let query = query_builder.build().await;
    println!("Query: {:#?}", query);

    println!("\n=== Setting up initial data ===");
    for source_change in get_initial_data() {
        _ = query.process_source_change(source_change).await;
    }
    println!("Initial data loaded: 2 vehicles (Blue and Red) and 1 zone (Parking Lot)");

    println!("\n=== Change 1: Testing initial state ===");
    println!("Initial data loaded: 2 vehicles (v1: 50000 miles, v2: 30000 miles)");
    println!("Note: Results are obtained through source changes, not direct query");

    println!("\n=== Change 2: Updating vehicle properties ===");
    println!("Updating Vehicle v1: Changing miles from 50000 to 75000");
    println!(
        "Result: {:?}",
        query
            .process_source_change(SourceChange::Update {
                element: Element::Node {
                    metadata: ElementMetadata {
                        reference: ElementReference::new("", "v1"),
                        labels: Arc::new([Arc::from("Vehicle")]),
                        effective_from: 0,
                    },
                    properties: ElementPropertyMap::from(json!({
                        "plate": "AAA-1234",
                        "color": "Blue",
                        "miles": 75000
                    }))
                },
            })
            .await
            .unwrap()
    );

    println!("\n=== Change 3: Updating vehicle color ===");
    println!("Updating Vehicle v1: Changing color from 'Blue' to 'Red' (keeping miles at 75000)");
    println!(
        "Result: {:?}",
        query
            .process_source_change(SourceChange::Update {
                element: Element::Node {
                    metadata: ElementMetadata {
                        reference: ElementReference::new("", "v1"),
                        labels: Arc::new([Arc::from("Vehicle")]),
                        effective_from: 0,
                    },
                    properties: ElementPropertyMap::from(json!({
                        "plate": "AAA-1234",
                        "color": "Red",
                        "miles": 75000
                    }))
                },
            })
            .await
            .unwrap()
    );
}

fn get_initial_data() -> Vec<SourceChange> {
    vec![
        // Vehicle v1: Blue vehicle with plate AAA-1234 and 50000 miles
        SourceChange::Insert {
            element: Element::Node {
                metadata: ElementMetadata {
                    reference: ElementReference::new("", "v1"),
                    labels: Arc::new([Arc::from("Vehicle")]),
                    effective_from: 0,
                },
                properties: ElementPropertyMap::from(json!({
                    "plate": "AAA-1234",
                    "color": "Blue",
                    "miles": 50000
                })),
            },
        },
        // Vehicle v2: Red vehicle with plate ZZZ-7890 and 30000 miles
        SourceChange::Insert {
            element: Element::Node {
                metadata: ElementMetadata {
                    reference: ElementReference::new("", "v2"),
                    labels: Arc::new([Arc::from("Vehicle")]),
                    effective_from: 0,
                },
                properties: ElementPropertyMap::from(json!({
                    "plate": "ZZZ-7890",
                    "color": "Red",
                    "miles": 30000
                })),
            },
        },
        // Zone z1: Parking Lot zone
        SourceChange::Insert {
            element: Element::Node {
                metadata: ElementMetadata {
                    reference: ElementReference::new("", "z1"),
                    labels: Arc::new([Arc::from("Zone")]),
                    effective_from: 0,
                },
                properties: ElementPropertyMap::from(json!({
                    "type": "Parking Lot"
                })),
            },
        },
    ]
}


fn create_hardcoded_ast() -> Query {
    Query {
        parts: vec![
            // First query part: Match with empty where clause
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation {
                            name: Some(Arc::from("v")),
                        },
                        labels: vec![Arc::from("Vehicle")],
                        property_predicates: vec![],
                    },
                    path: vec![], // No relationships, just match vehicles
                    optional: false,
                }],
                where_clauses: vec![], // Empty where clause
                return_clause: ProjectionClause::Item(vec![
                    Expression::UnaryExpression(UnaryExpression::Identifier(Arc::from("v"))),
                ]),
            },
            // Second query part: Filter by miles
            QueryPart {
                match_clauses: vec![], // No new matches
                where_clauses: vec![Expression::BinaryExpression(BinaryExpression::Gt(
                    Box::new(Expression::UnaryExpression(UnaryExpression::ExpressionProperty {
                        exp: Box::new(Expression::UnaryExpression(UnaryExpression::Identifier(
                            Arc::from("v"),
                        ))),
                        key: Arc::from("miles"),
                    })),
                    Box::new(Expression::UnaryExpression(UnaryExpression::Literal(
                        Literal::Integer(60000), // Filter for vehicles with more than 60000 miles
                    ))),
                ))],
                return_clause: ProjectionClause::Item(vec![
                    Expression::UnaryExpression(UnaryExpression::Identifier(Arc::from("v"))),
                ]),
            },
            // Third query part: Filter by color and final projection
            QueryPart {
                match_clauses: vec![], // No new matches
                where_clauses: vec![Expression::BinaryExpression(BinaryExpression::Eq(
                    Box::new(Expression::UnaryExpression(UnaryExpression::ExpressionProperty {
                        exp: Box::new(Expression::UnaryExpression(UnaryExpression::Identifier(
                            Arc::from("v"),
                        ))),
                        key: Arc::from("color"),
                    })),
                    Box::new(Expression::UnaryExpression(UnaryExpression::Literal(
                        Literal::Text(Arc::from("Red")), // Filter for red vehicles
                    ))),
                ))],
                return_clause: ProjectionClause::Item(vec![Expression::UnaryExpression(
                    UnaryExpression::ExpressionProperty {
                        exp: Box::new(Expression::UnaryExpression(UnaryExpression::Identifier(
                            Arc::from("v"),
                        ))),
                        key: Arc::from("color"),
                    },
                )]),
            },
        ],
    }
}