use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::models::product::Product;

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
    pub list: Vec<ProductResponse>,
    pub total: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CachedProductItem {
    pub id: i64,
    pub name: String,
    pub price: i64,
    pub sku: String,
}

impl From<Product> for ProductResponse {
    fn from(p: Product) -> Self {
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

impl From<&CachedProductItem> for ProductResponse {
    fn from(cached: &CachedProductItem) -> Self {
        ProductResponse {
            id: cached.id,
            name: cached.name.clone(),
            description: None,
            price: cached.price,
            sku: cached.sku.clone(),
            created_at: None,
            updated_at: None,
        }
    }
}

impl From<Product> for CachedProductItem {
    fn from(p: Product) -> Self {
        CachedProductItem {
            id: p.id,
            name: p.name,
            price: p.price,
            sku: p.sku,
        }
    }
}

impl From<&Product> for CachedProductItem {
    fn from(p: &Product) -> Self {
        CachedProductItem {
            id: p.id,
            name: p.name.clone(),
            price: p.price,
            sku: p.sku.clone(),
        }
    }
}
