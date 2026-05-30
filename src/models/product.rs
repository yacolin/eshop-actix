
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::FromRow;
use serde::Serialize;

#[derive(Debug, FromRow, Serialize)]
pub struct Product {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub sku: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<NaiveDateTime>,
}