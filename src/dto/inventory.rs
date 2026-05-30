use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::models::inventory::Inventory;

#[derive(Debug, Deserialize)]
pub struct CreateInventoryRequest {
    pub product_id: i64,
    pub quantity: i64,
    pub reserved: Option<i64>,
    pub threshold: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateInventoryRequest {
    pub quantity: Option<i64>,
    pub status: Option<String>,
    pub reserved: Option<i64>,
    pub threshold: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct InventoryListQuery {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InventoryResponse {
    pub id: i64,
    pub product_id: i64,
    pub quantity: i64,
    pub status: String,
    pub reserved: i64,
    pub threshold: i64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InventoryListResponse {
    pub list: Vec<InventoryResponse>,
    pub total: i64,
}

impl From<Inventory> for InventoryResponse {
    fn from(i: Inventory) -> Self {
        InventoryResponse {
            id: i.id,
            product_id: i.product_id,
            quantity: i.quantity,
            status: i.status,
            reserved: i.reserved,
            threshold: i.threshold,
            created_at: i.created_at,
            updated_at: i.updated_at,
        }
    }
}