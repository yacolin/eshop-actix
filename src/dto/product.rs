use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct CreateProductRequest {
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub sku: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateProductRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub price: Option<i64>,
    pub sku: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ProductListQuery {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
    pub keyword: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ProductResponse {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub sku: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct ProductListResponse {
    pub items: Vec<ProductResponse>,
    pub total: i64,
}

impl From<crate::models::product::Product> for ProductResponse {
    fn from(p: crate::models::product::Product) -> Self {
        ProductResponse {
            id: p.id,
            name: p.name,
            description: p.description,
            price: p.price,
            sku: p.sku,
            created_at: p.created_at,
            updated_at: p.updated_at,
        }
    }
}
