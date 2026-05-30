use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::FromRow;
use serde::Serialize;

#[derive(Debug, FromRow, Serialize)]
pub struct Inventory {
    pub id: i64,
    pub product_id: i64,
    pub quantity: i64,
    pub status: String,
    pub reserved: i64,
    pub threshold: i64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<NaiveDateTime>,
}