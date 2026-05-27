#![cfg(feature = "integration_tests")]
#![allow(deprecated)]
#![allow(clippy::needless_update)]

use bigtable_rs::bigtable::sql::{SqlType, ValueExt};
use bigtable_rs::bigtable::{BigTableConnection, Error};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::value::Kind;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    ArrayValue, ExecuteQueryRequest, Value,
};
use std::collections::HashMap;

async fn get_test_client() -> bigtable_rs::bigtable::BigTable {
    let connection = BigTableConnection::new_with_emulator(
        "127.0.0.1:9099",
        "mock-project",
        "mock-instance",
        false,
        1,
        None,
    )
    .expect("Failed to create connection");
    connection.client()
}

#[tokio::test]
async fn test_stable_scalar_inference() {
    // 1. Stable Primitive Scalar Mapping Test
    let mut client = get_test_client().await;

    let mut params = HashMap::new();
    params.insert(
        "string_param".to_string(),
        Value {
            kind: Some(Kind::StringValue("test-string".to_string())),
            r#type: None,
            ..Default::default()
        },
    );
    params.insert(
        "int_param".to_string(),
        Value {
            kind: Some(Kind::IntValue(12345)),
            r#type: None,
            ..Default::default()
        },
    );
    params.insert(
        "bool_param".to_string(),
        Value {
            kind: Some(Kind::BoolValue(true)),
            r#type: None,
            ..Default::default()
        },
    );

    let request = ExecuteQueryRequest {
        instance_name: "projects/mock-project/instances/mock-instance".to_string(),
        query: "SELECT * FROM my_table WHERE col = @string_param".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        !matches!(err, Error::ParameterTypeInferenceFailed(_, _)),
        "Expected network error, got parameter inference error: {:?}",
        err
    );
}

#[tokio::test]
async fn test_explicit_type_bypass() {
    // 2. Explicit Type Bypass Override Test
    let mut client = get_test_client().await;

    let mut params = HashMap::new();
    params.insert(
        "float_param".to_string(),
        Value {
            kind: Some(Kind::FloatValue(123.45)),
            r#type: None,
            ..Default::default()
        }
        .with_type(SqlType::Float64),
    );
    params.insert(
        "null_param".to_string(),
        Value {
            kind: None,
            r#type: None,
            ..Default::default()
        }
        .with_type(SqlType::String),
    );
    params.insert(
        "array_param".to_string(),
        Value {
            kind: Some(Kind::ArrayValue(ArrayValue::default())),
            r#type: None,
            ..Default::default()
        }
        .with_type(SqlType::Array(Box::new(SqlType::Int64))),
    );

    let request = ExecuteQueryRequest {
        instance_name: "projects/mock-project/instances/mock-instance".to_string(),
        query: "SELECT @float_param, @null_param, @array_param".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        !matches!(err, Error::ParameterTypeInferenceFailed(_, _)),
        "Explicit bypass failed, got parameter inference error: {:?}",
        err
    );
}

#[tokio::test]
async fn test_safeguard_null_inference_rejection() {
    // 3. Null/None Parameter Dynamic Inference Safeguard Rejection Test
    let mut client = get_test_client().await;

    let mut params = HashMap::new();
    params.insert(
        "bad_null".to_string(),
        Value {
            kind: None,
            r#type: None,
            ..Default::default()
        },
    );

    let request = ExecuteQueryRequest {
        instance_name: "projects/mock-project/instances/mock-instance".to_string(),
        query: "SELECT @bad_null".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::ParameterTypeInferenceFailed(ref field, ref msg) if field == "bad_null" && msg.contains("Null/None")),
        "Expected ParameterTypeInferenceFailed for bad_null, got: {:?}",
        err
    );
}

#[tokio::test]
async fn test_safeguard_float_inference_rejection() {
    // 4. Float Parameter Dynamic Inference Safeguard Rejection Test
    let mut client = get_test_client().await;

    let mut params = HashMap::new();
    params.insert(
        "bad_float".to_string(),
        Value {
            kind: Some(Kind::FloatValue(123.45)),
            r#type: None,
            ..Default::default()
        },
    );

    let request = ExecuteQueryRequest {
        instance_name: "projects/mock-project/instances/mock-instance".to_string(),
        query: "SELECT @bad_float".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::ParameterTypeInferenceFailed(ref field, ref msg) if field == "bad_float" && msg.contains("float")),
        "Expected ParameterTypeInferenceFailed for bad_float, got: {:?}",
        err
    );
}

#[tokio::test]
async fn test_safeguard_array_inference_rejection() {
    // 5. Array Parameter Dynamic Inference Safeguard Rejection Test
    let mut client = get_test_client().await;

    let mut params = HashMap::new();
    params.insert(
        "bad_array".to_string(),
        Value {
            kind: Some(Kind::ArrayValue(ArrayValue::default())),
            r#type: None,
            ..Default::default()
        },
    );

    let request = ExecuteQueryRequest {
        instance_name: "projects/mock-project/instances/mock-instance".to_string(),
        query: "SELECT @bad_array".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::ParameterTypeInferenceFailed(ref field, ref msg) if field == "bad_array" && msg.contains("ARRAY")),
        "Expected ParameterTypeInferenceFailed for bad_array, got: {:?}",
        err
    );
}

#[tokio::test]
async fn test_safeguard_struct_inference_rejection() {
    // 6. Struct Parameter Dynamic Inference Safeguard Rejection Test
    let mut client = get_test_client().await;

    let mut params = HashMap::new();
    params.insert(
        "bad_struct".to_string(),
        Value {
            kind: Some(Kind::ArrayValue(ArrayValue::default())),
            r#type: None,
            ..Default::default()
        },
    );

    let request = ExecuteQueryRequest {
        instance_name: "projects/mock-project/instances/mock-instance".to_string(),
        query: "SELECT @bad_struct".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::ParameterTypeInferenceFailed(ref field, ref msg) if field == "bad_struct" && msg.contains("STRUCT")),
        "Expected ParameterTypeInferenceFailed for bad_struct, got: {:?}",
        err
    );
}

#[tokio::test]
async fn test_explicit_struct_type_bypass() {
    // 7. Explicit Struct Type Bypass Override Test
    let mut client = get_test_client().await;

    let mut params = HashMap::new();
    params.insert(
        "struct_param".to_string(),
        Value {
            kind: Some(Kind::ArrayValue(ArrayValue::default())),
            r#type: None,
            ..Default::default()
        }
        .with_type(SqlType::Struct(vec![
            ("name".to_string(), SqlType::String),
            ("age".to_string(), SqlType::Int64),
        ])),
    );

    let request = ExecuteQueryRequest {
        instance_name: "projects/mock-project/instances/mock-instance".to_string(),
        query: "SELECT @struct_param".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        !matches!(err, Error::ParameterTypeInferenceFailed(_, _)),
        "Explicit Struct bypass failed, got parameter inference error: {:?}",
        err
    );
}

#[tokio::test]
async fn test_explicit_map_type_bypass() {
    // 8. Explicit Map Type Bypass Override Test
    let mut client = get_test_client().await;

    let mut params = HashMap::new();
    params.insert(
        "map_param".to_string(),
        Value {
            kind: Some(Kind::ArrayValue(ArrayValue::default())),
            r#type: None,
            ..Default::default()
        }
        .with_type(SqlType::Map {
            key_type: Box::new(SqlType::String),
            value_type: Box::new(SqlType::Int64),
        }),
    );

    let request = ExecuteQueryRequest {
        instance_name: "projects/mock-project/instances/mock-instance".to_string(),
        query: "SELECT @map_param".to_string(),
        params,
        ..Default::default()
    };

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        !matches!(err, Error::ParameterTypeInferenceFailed(_, _)),
        "Explicit Map bypass failed, got parameter inference error: {:?}",
        err
    );
}
