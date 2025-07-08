// Copyright 2025 The Drasi Authors.
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

use std::collections::HashSet;

use super::*;
use ast::*;
use drasi_query_cypher::{parse, CypherConfiguration};

struct TestConfig {}

impl GQLConfiguration for TestConfig {
    fn get_aggregating_function_names(&self) -> HashSet<String> {
        let mut set = HashSet::new();
        set.insert("count".into());
        set.insert("sum".into());
        set.insert("min".into());
        set.insert("max".into());
        set.insert("avg".into());
        set
    }
}

static TEST_CONFIG: TestConfig = TestConfig {};

// GROUP BY tests
#[test]
fn implicit_grouping_with_one_key() {
    // 1. Implicit Grouping with One Key
    // Expected: Groups by z.type (zone_type), counts vehicles.
    let query = gql::query(
        "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone {type:'Parking Lot'})
        RETURN z.type AS zone_type, count(v) AS vehicle_count",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![
                UnaryExpression::alias(
                    UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                    "zone_type".into()
                )
            ],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("v")], 99),
                    "vehicle_count".into()
                )
            ]
        }
    );
}

#[test]
fn implicit_grouping_with_two_keys() {
    // 2. Implicit Grouping with Two Keys
    // Checks that multiple non-aggregated expressions in RETURN are all treated as grouping keys.
    let query = gql::query(
        "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone {type:'Parking Lot'})
         RETURN z.type AS zone_type, v.color AS vehicle_color, count(v) AS vehicle_count",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![
                UnaryExpression::alias(
                    UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                    "zone_type".into()
                ),
                UnaryExpression::alias(
                    UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                    "vehicle_color".into()
                )
            ],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("v")], 126),
                    "vehicle_count".into()
                )
            ]
        }
    );
}

#[test]
fn explicit_group_by_all_keys_projected() {
    // 3. Explicit GROUP BY: All Keys Projected
    // Ensures explicit GROUP BY behaves identically to implicit grouping.
    let query = gql::query(
        "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         RETURN z.type AS zone_type, v.color AS vehicle_color, count(v) AS vehicle_count
         GROUP BY zone_type, vehicle_color",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![
                UnaryExpression::alias(
                    UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                    "zone_type".into()
                ),
                UnaryExpression::alias(
                    UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                    "vehicle_color".into()
                )
            ],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("v")], 105),
                    "vehicle_count".into()
                )
            ]
        }
    );
}

#[test]
fn explicit_group_by_subset_of_keys_projected() {
    // 4. Explicit GROUP BY: Subset of Keys Projected
    // Creates a multi-part query (like Cypher's WITH) in the AST.
    let query = gql::query(
        "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         RETURN z.type AS zone_type, count(v) AS vehicle_count
         GROUP BY zone_type, v.color",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query,
        Query {
            parts: vec![
                // First part: Group by all specified keys
                QueryPart {
                    match_clauses: vec![MatchClause {
                        start: NodeMatch::with_annotation(Annotation::new("v".into()), "Vehicle".into()),
                        path: vec![(
                            RelationMatch::right(Annotation::empty(), vec!["LOCATED_IN".into()], vec![], None),
                            NodeMatch::with_annotation(Annotation::new("z".into()), "Zone".into())
                        )],
                        optional: false,
                    }],
                    where_clauses: vec![],
                    return_clause: ProjectionClause::GroupBy {
                        grouping: vec![
                            UnaryExpression::alias(
                                UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                                "zone_type".into()
                            ),
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into())
                        ],
                        aggregates: vec![
                            UnaryExpression::alias(
                                FunctionExpression::function("count".into(), vec![UnaryExpression::ident("v")], 79),
                                "vehicle_count".into()
                            )
                        ]
                    }
                },
                // Second part: Final projection with only subset of keys
                QueryPart {
                    match_clauses: vec![],
                    where_clauses: vec![],
                    return_clause: ProjectionClause::Item(vec![
                        UnaryExpression::ident("zone_type"),
                        UnaryExpression::ident("vehicle_count")
                    ])
                }
            ]
        }
    );
}

#[test]
fn group_by_with_function_expression() {
    // 5. GROUP BY with Function Expression
    // Verifies that functions can be used as grouping keys.
    let query = gql::query(
        "MATCH (a)-[t:Transfers]->(b)
         RETURN FLOOR(t.amount) AS amount_group, count(t) AS number_of_transfers
         GROUP BY amount_group",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("FLOOR".into(), vec![
                        UnaryExpression::expression_property(UnaryExpression::ident("t"), "amount".into())
                    ], 45),
                    "amount_group".into()
                )
            ],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("t")], 78),
                    "number_of_transfers".into()
                )
            ]
        }
    );
}

#[test]
fn group_by_with_binary_expression() {
    // 6. GROUP BY with Binary Expression
    // Ensures that binary expressions can serve as grouping keys.
    let query = gql::query(
        "MATCH (a)-[t:Transfers]->(b)
         RETURN t.amount + 100, count(t) AS number_of_transfers
         GROUP BY t.amount + 100",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![
                BinaryExpression::add(
                    UnaryExpression::expression_property(UnaryExpression::ident("t"), "amount".into()),
                    UnaryExpression::literal(Literal::Integer(100))
                )
            ],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("t")], 61),
                    "number_of_transfers".into()
                )
            ]
        }
    );
}

#[test]
fn group_by_with_aliased_column() {
    // 8. GROUP BY with Aliased Column
    // Tests that aliases specified in RETURN can be referenced in the GROUP BY clause.
    let query = gql::query(
        "MATCH (a)-[t:Transfers]->(b)
         RETURN t.account_id AS account, count(t) AS number_of_transfers
         GROUP BY account",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![
                UnaryExpression::alias(
                    UnaryExpression::expression_property(UnaryExpression::ident("t"), "account_id".into()),
                    "account".into()
                )
            ],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("t")], 70),
                    "number_of_transfers".into()
                )
            ]
        }
    );
}

#[test]
fn group_by_with_account_id_and_count() {
    // 9. GROUP BY with Account ID and Count
    // Tests grouping by account_id and returning both the grouping key and aggregate count.
    let query = gql::query(
        "MATCH (a)-[t:Transfers]->(b)
            RETURN t.account_id, count(t) AS number_of_transfers
            GROUP BY t.account_id",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query,
        Query {
            parts: vec![
                QueryPart {
                    match_clauses: vec![MatchClause {
                        start: NodeMatch::new(Annotation::new("a".into()), vec![], vec![]),
                        path: vec![(
                            RelationMatch::right(Annotation::new("t".into()), vec!["Transfers".into()], vec![], None),
                            NodeMatch::new(Annotation::new("b".into()), vec![], vec![])
                        )],
                        optional: false,
                    }],
                    where_clauses: vec![],
                    return_clause: ProjectionClause::GroupBy {
                        grouping: vec![
                            UnaryExpression::expression_property(UnaryExpression::ident("t"), "account_id".into())
                        ],
                        aggregates: vec![
                            UnaryExpression::alias(
                                FunctionExpression::function("count".into(), vec![UnaryExpression::ident("t")], 62),
                                "number_of_transfers".into()
                            )
                        ]
                    }
                }
            ]
        }
    );
}

#[test]
fn group_by_empty() {
    // 10. GROUP BY ()
    // Tests the special case where GROUP BY () groups all rows into a single group.
    let query = gql::query(
        "MATCH (v:Vehicle) RETURN count(v) AS total_rows GROUP BY ()",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("v")], 25),
                    "total_rows".into()
                )
            ]
        }
    );
}

#[test]
fn implicit_grouping_with_only_aggregates() {
    // 12. Implicit Grouping with Only Aggregates
    // Tests that when RETURN contains only aggregate functions with no explicit GROUP BY,
    // it should infer a single-group aggregation (empty grouping key set) just like GROUP BY ().
    let query = gql::query(
        "MATCH (v:Vehicle) 
         RETURN count(v) AS total",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("v")], 35),
                    "total".into()
                )
            ]
        }
    );
}

#[test]
fn grouping_on_raw_identifiers() {
    // 13. Grouping on Raw Identifiers (No Alias)
    // Tests that GROUP BY can reference un-aliased expressions from the RETURN clause.
    let query = gql::query(
        "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone) 
         RETURN z.type, count(v) AS vehicle_count 
         GROUP BY z.type",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![
                UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into())
            ],
            aggregates: vec![
                UnaryExpression::alias(
                    FunctionExpression::function("count".into(), vec![UnaryExpression::ident("v")], 67),
                    "vehicle_count".into()
                )
            ]
        }
    );
}

#[test]
fn grouping_on_non_aliased_function() {
    // 14. Grouping on Non-aliased Function
    // Tests that GROUP BY can reference un-aliased function expressions from the RETURN clause.
    let query = gql::query(
        "MATCH (a)-[t:Transfers]->(b) 
         RETURN floor(t.amount), count(t) 
         GROUP BY floor(t.amount)",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query.parts[0].return_clause,
        ProjectionClause::GroupBy {
            grouping: vec![
                FunctionExpression::function("floor".into(), vec![
                    UnaryExpression::expression_property(UnaryExpression::ident("t"), "amount".into())
                ], 46)
            ],
            aggregates: vec![
                FunctionExpression::function("count".into(), vec![UnaryExpression::ident("t")], 63)
            ]
        }
    );
}
#[test]
fn group_by_and_where_on_vehicles() {
    // This test checks GROUP BY and WHERE together
    let query = gql::query(
        "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         WHERE v.color = 'Red'
         RETURN z.type, count(v) AS vehicle_count
         GROUP BY z.type
        ",
        &TEST_CONFIG,
    )
    .unwrap();

    assert_eq!(
        query,
        Query {
            parts: vec![
                QueryPart {
                    match_clauses: vec![
                        MatchClause {
                            start: NodeMatch {
                                annotation: Annotation {
                                    name: Some("v".into()),
                                },
                                labels: vec!["Vehicle".into()],
                                property_predicates: vec![],
                            },
                            path: vec![
                                (
                                    RelationMatch {
                                        direction: Direction::Right,
                                        annotation: Annotation {
                                            name: None,
                                        },
                                        variable_length: None,
                                        labels: vec!["LOCATED_IN".into()],
                                        property_predicates: vec![],
                                    },
                                    NodeMatch {
                                        annotation: Annotation {
                                            name: Some("z".into()),
                                        },
                                        labels: vec!["Zone".into()],
                                        property_predicates: vec![],
                                    },
                                ),
                            ],
                            optional: false,
                        },
                    ],
                    where_clauses: vec![
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(
                                UnaryExpression::ident("v"),
                                "color".into()
                            ),
                            UnaryExpression::literal(Literal::Text("Red".into()))
                        ),
                    ],
                    return_clause: ProjectionClause::GroupBy {
                        grouping: vec![
                            UnaryExpression::expression_property(
                                UnaryExpression::ident("z"),
                                "type".into()
                            ),
                        ],
                        aggregates: vec![
                            UnaryExpression::alias(
                                FunctionExpression::function(
                                    "count".into(),
                                    vec![UnaryExpression::ident("v")],
                                    97
                                ),
                                "vehicle_count".into()
                            ),
                        ],
                    },
                },
            ],
        }
    );
}


// LET and YIELD Tests
// ZoneVehicleGraph Tests

// Shared Cypher test config for AST comparison
struct TestCypherConfig {}
impl CypherConfiguration for TestCypherConfig {
    fn get_aggregating_function_names(&self) -> std::collections::HashSet<String> {
        let mut set = std::collections::HashSet::new();
        set.insert("count".into());
        set.insert("sum".into());
        set.insert("min".into());
        set.insert("max".into());
        set.insert("avg".into());
        set
    }
}

#[test]
fn simple_let_and_return() {
    let gql_query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         LET isRed = v.color = 'Red'
         RETURN v.color, z.type, isRed";
    let cypher_query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
        WITH v, z, v.color = 'Red' AS isRed
        RETURN v.color, z.type, isRed";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();

    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}

#[test]
fn example_1_simple_let_assignment() {
    let gql_query = "MATCH (v:Vehicle)
         LET isRed = v.color = 'Red'
         RETURN v.color, isRed";
    let cypher_query = "MATCH (v:Vehicle)
        WITH v, v.color = 'Red' AS isRed
        RETURN v.color, isRed";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();

    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}

#[test]
fn example_2_let_with_expression() {
    let gql_query = "MATCH (a:Account)
         LET isVacation = a.nick_name = 'Vacation Fund'
         RETURN a.nick_name, isVacation";
    let cypher_query = "MATCH (a:Account)
        WITH a, a.nick_name = 'Vacation Fund' AS isVacation
        RETURN a.nick_name, isVacation";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();

    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}

#[test]
fn example_3_multiple_let_variables_in_one_clause() {
    let gql_query = "MATCH (a:Account)
         LET active = a.is_blocked = false, nameLength = LENGTH(a.nick_name)
         RETURN a.nick_name, active, nameLength";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    
    let expected_ast = Query {
        parts: vec![
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation { name: Some("a".into()) },
                        labels: vec!["Account".into()],
                        property_predicates: vec![],
                    },
                    path: vec![],
                    optional: false,
                }],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("a"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("a"), "is_blocked".into()),
                            UnaryExpression::literal(Literal::Boolean(false))
                        ),
                        "active".into()
                    ),
                    UnaryExpression::alias(
                        FunctionExpression::function(
                            "LENGTH".into(),
                            vec![UnaryExpression::expression_property(UnaryExpression::ident("a"), "nick_name".into())],
                            75
                        ),
                        "nameLength".into()
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::expression_property(UnaryExpression::ident("a"), "nick_name".into()),
                    UnaryExpression::ident("active"),
                    UnaryExpression::ident("nameLength"),
                ]),
            },
        ],
    };

    assert_eq!(gql_ast, expected_ast, "GQL AST should match expected structure");
}

#[test]
fn example_4_chained_let_clauses_preserving_all_variables() {
    let gql_query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         LET isRed = v.color = 'Red'
         LET inGarage = z.type = 'Garage'
         RETURN v.color, z.type, isRed, inGarage";
    let cypher_query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
        WITH v, z, v.color = 'Red' AS isRed
        WITH v, z, isRed, z.type = 'Garage' AS inGarage
        RETURN v.color, z.type, isRed, inGarage";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();


    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}

#[test]
fn test_let_with_where_clause() {
    let gql_query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
    WHERE z.type = 'Garage'
    LET color = v.color
    RETURN color";
    let cypher_query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
    WHERE z.type = 'Garage'
    WITH v, z, v.color as color
    RETURN color";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();

    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}

#[test]
fn example_5_let_with_conditionals() {
    let gql_query = "MATCH (a:Account)
         LET status = CASE WHEN a.is_blocked THEN 'Blocked' ELSE 'Active' END
         RETURN a.nick_name, status";
    let cypher_query = "MATCH (a:Account)
        WITH a, CASE WHEN a.is_blocked THEN 'Blocked' ELSE 'Active' END AS status
        RETURN a.nick_name, status";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();

    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}

#[test]
fn example_6_chained_lets_with_multiple_new_variables() {
    let gql_query = "MATCH (p:Person)
         LET nameLength = LENGTH(p.name)
         LET isShortName = nameLength < 5, isLongName = nameLength > 7
         RETURN p.name, isShortName, isLongName";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    
    let expected_ast = Query {
        parts: vec![
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation { name: Some("p".into()) },
                        labels: vec!["Person".into()],
                        property_predicates: vec![],
                    },
                    path: vec![],
                    optional: false,
                }],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("p"),
                    UnaryExpression::alias(
                        FunctionExpression::function(
                            "LENGTH".into(),
                            vec![UnaryExpression::expression_property(UnaryExpression::ident("p"), "name".into())],
                            43
                        ),
                        "nameLength".into()
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("p"),
                    UnaryExpression::ident("nameLength"),
                    UnaryExpression::alias(
                        BinaryExpression::lt(
                            UnaryExpression::ident("nameLength"),
                            UnaryExpression::literal(Literal::Integer(5))
                        ),
                        "isShortName".into()
                    ),
                    UnaryExpression::alias(
                        BinaryExpression::gt(
                            UnaryExpression::ident("nameLength"),
                            UnaryExpression::literal(Literal::Integer(7))
                        ),
                        "isLongName".into()
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::expression_property(UnaryExpression::ident("p"), "name".into()),
                    UnaryExpression::ident("isShortName"),
                    UnaryExpression::ident("isLongName"),
                ]),
            },
        ],
    };

    assert_eq!(gql_ast, expected_ast, "GQL AST should match expected structure");
}

// GROUP BY with LET tests

#[test]
fn group_by_let_defined_variable() {
    // Example 1: Group by LET-Defined Variable
    // MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
    // WITH v, z, v.color = 'Red' AS isRed
    // RETURN z.type AS zone_type, isRed, count(v) AS vehicle_count

    let query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         LET isRed = v.color = 'Red'
         RETURN z.type AS zone_type, isRed, count(v) AS vehicle_count
         GROUP BY zone_type, isRed";

    let gql_ast = gql::query(query, &TEST_CONFIG).unwrap();

    let expected_ast = Query {
        parts: vec![
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation { name: Some("v".into()) },
                        labels: vec!["Vehicle".into()],
                        property_predicates: vec![],
                    },
                    path: vec![
                        (
                            RelationMatch::right(Annotation::empty(), vec!["LOCATED_IN".into()], vec![], None),
                            NodeMatch {
                                annotation: Annotation { name: Some("z".into()) },
                                labels: vec!["Zone".into()],
                                property_predicates: vec![],
                            },
                        ),
                    ],
                    optional: false,
                }],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("v"),
                    UnaryExpression::ident("z"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                            UnaryExpression::literal(Literal::Text("Red".into())),
                        ),
                        "isRed".into(),
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::GroupBy {
                    grouping: vec![
                        UnaryExpression::alias(
                            UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                            "zone_type".into(),
                        ),
                        UnaryExpression::ident("isRed"),
                    ],
                    aggregates: vec![
                        UnaryExpression::alias(
                            FunctionExpression::function(
                                "count".into(),
                                vec![UnaryExpression::ident("v")],
                                123,
                            ),
                            "vehicle_count".into(),
                        ),
                    ],
                },
            },
        ],
    };

    assert_eq!(gql_ast, expected_ast, "GQL AST should match expected structure");
}

#[test]
fn multiple_let_variables_in_group_by() {
    // Example 2: Multiple LET Variables in GROUP BY
    // MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
    // WITH v, z, v.color = 'Red' AS isRed
    // WITH v, z, isRed, v.color = 'Blue' AS isBlue
    // RETURN zone_type, isRed, isBlue, count(v) AS vehicle_count

    let query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         LET isRed = v.color = 'Red'
         LET isBlue = v.color = 'Blue'
         RETURN z.type AS zone_type, isRed, isBlue, count(v) AS vehicle_count
         GROUP BY zone_type, isRed, isBlue";
    
    let gql_ast = gql::query(query, &TEST_CONFIG).unwrap();

    let expected_ast = Query {
        parts: vec![
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation { name: Some("v".into()) },
                        labels: vec!["Vehicle".into()],
                        property_predicates: vec![],
                    },
                    path: vec![
                        (
                            RelationMatch::right(Annotation::empty(), vec!["LOCATED_IN".into()], vec![], None),
                            NodeMatch {
                                annotation: Annotation { name: Some("z".into()) },
                                labels: vec!["Zone".into()],
                                property_predicates: vec![],
                            },
                        ),
                    ],
                    optional: false,
                }],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("v"),
                    UnaryExpression::ident("z"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                            UnaryExpression::literal(Literal::Text("Red".into())),
                        ),
                        "isRed".into(),
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("v"),
                    UnaryExpression::ident("z"),
                    UnaryExpression::ident("isRed"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                            UnaryExpression::literal(Literal::Text("Blue".into())),
                        ),
                        "isBlue".into(),
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::GroupBy {
                    grouping: vec![
                        UnaryExpression::alias(
                            UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                            "zone_type".into(),
                        ),
                        UnaryExpression::ident("isRed"),
                        UnaryExpression::ident("isBlue"),
                    ],
                    aggregates: vec![
                        UnaryExpression::alias(
                            FunctionExpression::function(
                                "count".into(),
                                vec![UnaryExpression::ident("v")],
                                170,
                            ),
                            "vehicle_count".into(),
                        ),
                    ],
                },
            },
        ],
    };
    

    assert_eq!(gql_ast, expected_ast, "GQL AST should match expected structure");
}

#[test]
fn group_by_let_defined_variable_with_less_projected_columns() {
    // Example 3: Group by LET-Defined Variable with less Projected Columns
    // MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
    // WITH v, z, v.color = 'Red' AS isRed
    // WITH z.type AS zone_type, isRed, count(v) AS vehicle_count
    // RETURN zone_type, vehicle_count
    let query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         LET isRed = v.color = 'Red'
         RETURN z.type AS zone_type, count(v) AS vehicle_count
         GROUP BY zone_type, isRed";
    
    let gql_ast = gql::query(query, &TEST_CONFIG).unwrap();
    let expected_ast = Query {
        parts: vec![
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation { name: Some("v".into()) },
                        labels: vec!["Vehicle".into()],
                        property_predicates: vec![],
                    },
                    path: vec![
                        (
                            RelationMatch::right(Annotation::empty(), vec!["LOCATED_IN".into()], vec![], None),
                            NodeMatch {
                                annotation: Annotation { name: Some("z".into()) },
                                labels: vec!["Zone".into()],
                                property_predicates: vec![],
                            },
                        ),
                    ],
                    optional: false,
                }],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("v"),
                    UnaryExpression::ident("z"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                            UnaryExpression::literal(Literal::Text("Red".into())),
                        ),
                        "isRed".into(),
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::GroupBy {
                    grouping: vec![
                        UnaryExpression::alias(
                            UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                            "zone_type".into(),
                        ),
                        UnaryExpression::ident("isRed"),
                    ],
                    aggregates: vec![
                        UnaryExpression::alias(
                            FunctionExpression::function(
                                "count".into(),
                                vec![UnaryExpression::ident("v")],
                                116,
                            ),
                            "vehicle_count".into(),
                        ),
                    ],
                },
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("zone_type"),
                    UnaryExpression::ident("vehicle_count"),
                ]),
            },
        ],
    };
    
    assert_eq!(gql_ast, expected_ast, "GQL AST should match expected structure");
}

#[test]
fn implicit_grouping_with_let() {
    // Implicit grouping with LET
    // MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
    // WITH v, z, v.color = 'Red' AS isRed
    // RETURN z.type AS zone_type, isRed, count(v) AS vehicle_count
    let query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         LET isRed = v.color = 'Red'
         RETURN z.type AS zone_type, isRed, count(v) AS vehicle_count";
    let gql_ast = gql::query(query, &TEST_CONFIG).unwrap();
    let expected_ast = Query {
        parts: vec![
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation { name: Some("v".into()) },
                        labels: vec!["Vehicle".into()],
                        property_predicates: vec![],
                    },
                    path: vec![
                        (
                            RelationMatch::right(Annotation::empty(), vec!["LOCATED_IN".into()], vec![], None),
                            NodeMatch {
                                annotation: Annotation { name: Some("z".into()) },
                                labels: vec!["Zone".into()],
                                property_predicates: vec![],
                            },
                        ),
                    ],
                    optional: false,
                }],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("v"),
                    UnaryExpression::ident("z"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                            UnaryExpression::literal(Literal::Text("Red".into())),
                        ),
                        "isRed".into(),
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::GroupBy {
                    grouping: vec![
                        UnaryExpression::alias(
                            UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                            "zone_type".into(),
                        ),
                        UnaryExpression::ident("isRed"),
                    ],
                    aggregates: vec![
                        UnaryExpression::alias(
                            FunctionExpression::function(
                                "count".into(),
                                vec![UnaryExpression::ident("v")],
                                123,
                            ),
                            "vehicle_count".into(),
                        ),
                    ],
                },
            },
        ],
    };
    assert_eq!(gql_ast, expected_ast, "GQL AST should match expected structure");
}

#[test]
fn implicit_grouping_with_multiple_let() {
    // Implicit grouping with multiple LET
    // MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
    // WITH v, z, v.color = 'Red' AS isRed
    // WITH v, z, isRed, v.color = 'Blue' AS isBlue
    // RETURN z.type AS zone_type, isRed, isBlue, count(v) AS vehicle_count
    let query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         LET isRed = v.color = 'Red'
         LET isBlue = v.color = 'Blue'
         RETURN z.type AS zone_type, isRed, isBlue, count(v) AS vehicle_count";
    let gql_ast = gql::query(query, &TEST_CONFIG).unwrap();
    let expected_ast = Query {
        parts: vec![
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation { name: Some("v".into()) },
                        labels: vec!["Vehicle".into()],
                        property_predicates: vec![],
                    },
                    path: vec![
                        (
                            RelationMatch::right(Annotation::empty(), vec!["LOCATED_IN".into()], vec![], None),
                            NodeMatch {
                                annotation: Annotation { name: Some("z".into()) },
                                labels: vec!["Zone".into()],
                                property_predicates: vec![],
                            },
                        ),
                    ],
                    optional: false,
                }],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("v"),
                    UnaryExpression::ident("z"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                            UnaryExpression::literal(Literal::Text("Red".into())),
                        ),
                        "isRed".into(),
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("v"),
                    UnaryExpression::ident("z"),
                    UnaryExpression::ident("isRed"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                            UnaryExpression::literal(Literal::Text("Blue".into())),
                        ),
                        "isBlue".into(),
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::GroupBy {
                    grouping: vec![
                        UnaryExpression::alias(
                            UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                            "zone_type".into(),
                        ),
                        UnaryExpression::ident("isRed"),
                        UnaryExpression::ident("isBlue"),
                    ],
                    aggregates: vec![
                        UnaryExpression::alias(
                            FunctionExpression::function(
                                "count".into(),
                                vec![UnaryExpression::ident("v")],
                                170,
                            ),
                            "vehicle_count".into(),
                        ),
                    ],
                },
            },
        ],
    };
    assert_eq!(gql_ast, expected_ast, "GQL AST should match expected structure");
}

#[test]
fn let_variable_not_used_in_group_by_or_return() {
    // LET Variable Not Used in GROUP BY or RETURN
    // MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
    // WITH v, z, v.color = 'Red' AS isRed
    // RETURN z.type AS zone_type, count(v) AS vehicle_count

    let query = "MATCH (v:Vehicle)-[:LOCATED_IN]->(z:Zone)
         LET isRed = v.color = 'Red'
         RETURN z.type AS zone_type, count(v) AS vehicle_count
         GROUP BY zone_type";
    let gql_ast = gql::query(query, &TEST_CONFIG).unwrap();
    let expected_ast = Query {
        parts: vec![
            QueryPart {
                match_clauses: vec![MatchClause {
                    start: NodeMatch {
                        annotation: Annotation { name: Some("v".into()) },
                        labels: vec!["Vehicle".into()],
                        property_predicates: vec![],
                    },
                    path: vec![
                        (
                            RelationMatch::right(Annotation::empty(), vec!["LOCATED_IN".into()], vec![], None),
                            NodeMatch {
                                annotation: Annotation { name: Some("z".into()) },
                                labels: vec!["Zone".into()],
                                property_predicates: vec![],
                            },
                        ),
                    ],
                    optional: false,
                }],
                where_clauses: vec![],
                return_clause: ProjectionClause::Item(vec![
                    UnaryExpression::ident("v"),
                    UnaryExpression::ident("z"),
                    UnaryExpression::alias(
                        BinaryExpression::eq(
                            UnaryExpression::expression_property(UnaryExpression::ident("v"), "color".into()),
                            UnaryExpression::literal(Literal::Text("Red".into())),
                        ),
                        "isRed".into(),
                    ),
                ]),
            },
            QueryPart {
                match_clauses: vec![],
                where_clauses: vec![],
                return_clause: ProjectionClause::GroupBy {
                    grouping: vec![
                        UnaryExpression::alias(
                            UnaryExpression::expression_property(UnaryExpression::ident("z"), "type".into()),
                            "zone_type".into(),
                        ),
                    ],
                    aggregates: vec![
                        UnaryExpression::alias(
                            FunctionExpression::function(
                                "count".into(),
                                vec![UnaryExpression::ident("v")],
                                116,
                            ),
                            "vehicle_count".into(),
                        ),
                    ],
                },
            },
        ],
    };
    assert_eq!(gql_ast, expected_ast, "GQL AST should match expected structure");
}


// YIELD tests

#[test]
fn simple_yield() {
    let gql_query = "MATCH (v:Vehicle)-[e:LOCATED_IN]->(z:Zone)
         YIELD v.color AS vehicleColor, z.type AS location
         RETURN vehicleColor, location";
    let cypher_query = "MATCH (v:Vehicle)-[e:LOCATED_IN]->(z:Zone)
        WITH v.color AS vehicleColor, z.type AS location
        RETURN vehicleColor, location";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();

    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}

#[test]
fn yield_single_identifier() {
    let gql_query = "MATCH (v:Vehicle)-[e:LOCATED_IN]->(z:Zone)
         YIELD v
         RETURN v.color";
    let cypher_query = "MATCH (v:Vehicle)-[e:LOCATED_IN]->(z:Zone)
        WITH v
        RETURN v.color";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();

    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}

#[test]
fn yield_with_let_and_chained_yield() {
    let gql_query = "MATCH (p:Product)
         LET productName = p.name, cost = p.price
         YIELD productName, cost
         LET total = cost * 1.2
         YIELD total AS finalPrice
         RETURN finalPrice";
    let cypher_query = "MATCH (p:Product)
        WITH p, p.name AS productName, p.price AS cost
        WITH productName, cost
        WITH productName, cost, cost * 1.2 AS total
        WITH total AS finalPrice
        RETURN finalPrice";

    let gql_ast = gql::query(gql_query, &TEST_CONFIG).unwrap();
    let cypher_ast = parse(cypher_query, &TestCypherConfig {}).unwrap();

    assert_eq!(gql_ast, cypher_ast, "GQL and Cypher ASTs should be equal");
}


