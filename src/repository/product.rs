use sqlx::MySqlPool;

use crate::models::product::Product;

pub async fn find_by_id(pool: &MySqlPool, id: i64) -> Result<Option<Product>, sqlx::Error> {
    sqlx::query_as::<_, Product>(
        "SELECT id, name, description, price, sku, created_at, updated_at, deleted_at \
         FROM products WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn create(
    pool: &MySqlPool,
    name: &str,
    description: Option<&str>,
    price: i64,
    sku: &str,
) -> Result<Product, sqlx::Error> {
    sqlx::query(
        "INSERT INTO products (name, description, price, sku) VALUES (?, ?, ?, ?)",
    )
    .bind(name)
    .bind(description)
    .bind(price)
    .bind(sku)
    .execute(pool)
    .await?;

    find_by_sku(pool, sku)
        .await
        .map(|p| p.unwrap())
}

pub async fn update(
    pool: &MySqlPool,
    id: i64,
    name: Option<&str>,
    description: Option<&str>,
    price: Option<i64>,
    sku: Option<&str>,
) -> Result<Option<Product>, sqlx::Error> {
    let mut sets: Vec<String> = Vec::new();
    if name.is_some() {
        sets.push("name = ?".to_string());
    }
    if description.is_some() {
        sets.push("description = ?".to_string());
    }
    if price.is_some() {
        sets.push("price = ?".to_string());
    }
    if sku.is_some() {
        sets.push("sku = ?".to_string());
    }

    if sets.is_empty() {
        return find_by_id(pool, id).await;
    }

    let sql = format!(
        "UPDATE products SET {} WHERE id = ? AND deleted_at IS NULL",
        sets.join(", ")
    );

    let mut q = sqlx::query(&sql);
    if let Some(v) = name {
        q = q.bind(v);
    }
    if let Some(v) = description {
        q = q.bind(v);
    }
    if let Some(v) = price {
        q = q.bind(v);
    }
    if let Some(v) = sku {
        q = q.bind(v);
    }
    q = q.bind(id);

    let result = q.execute(pool).await?;
    if result.rows_affected() > 0 {
        find_by_id(pool, id).await
    } else {
        Ok(None)
    }
}

pub async fn soft_delete(pool: &MySqlPool, id: i64) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE products SET deleted_at = NOW(3) WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn find_list(
    pool: &MySqlPool,
    keyword: Option<&str>,
    offset: u32,
    limit: u32,
) -> Result<Vec<Product>, sqlx::Error> {
    let mut where_clause = String::from("WHERE deleted_at IS NULL");

    if keyword.is_some() {
        where_clause.push_str(" AND (name LIKE ? OR sku LIKE ?)");
    }

    let sql = format!(
        "SELECT id, name, description, price, sku, created_at, updated_at, deleted_at \
         FROM products {} ORDER BY id DESC LIMIT ? OFFSET ?",
        where_clause
    );

    let mut q = sqlx::query_as::<_, Product>(&sql);
    let pattern = keyword.map(|kw| format!("%{}%", kw));
    if let Some(ref pat) = pattern {
        q = q.bind(pat).bind(pat);
    }
    q = q.bind(limit).bind(offset);

    q.fetch_all(pool).await
}

pub async fn count_list(
    pool: &MySqlPool,
    keyword: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let mut where_clause = String::from("WHERE deleted_at IS NULL");

    if keyword.is_some() {
        where_clause.push_str(" AND (name LIKE ? OR sku LIKE ?)");
    }

    let sql = format!(
        "SELECT COUNT(*) FROM products {}",
        where_clause
    );

    let mut q = sqlx::query_scalar::<_, i64>(&sql);
    let pattern = keyword.map(|kw| format!("%{}%", kw));
    if let Some(ref pat) = pattern {
        q = q.bind(pat).bind(pat);
    }

    q.fetch_one(pool).await
}

pub async fn find_by_sku(pool: &MySqlPool, sku: &str) -> Result<Option<Product>, sqlx::Error> {
    sqlx::query_as::<_, Product>(
        "SELECT id, name, description, price, sku, created_at, updated_at, deleted_at \
         FROM products WHERE sku = ? AND deleted_at IS NULL",
    )
    .bind(sku)
    .fetch_optional(pool)
    .await
}