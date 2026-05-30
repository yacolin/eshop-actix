use sqlx::MySqlPool;

use crate::models::inventory::Inventory;

pub async fn find_by_id(pool: &MySqlPool, id: i64) -> Result<Option<Inventory>, sqlx::Error> {
    sqlx::query_as::<_, Inventory>(
        "SELECT id, product_id, quantity, status, reserved, threshold, created_at, updated_at, deleted_at \
         FROM inventories WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn find_by_product_id(
    pool: &MySqlPool,
    product_id: i64,
) -> Result<Option<Inventory>, sqlx::Error> {
    sqlx::query_as::<_, Inventory>(
        "SELECT id, product_id, quantity, status, reserved, threshold, created_at, updated_at, deleted_at \
         FROM inventories WHERE product_id = ? AND deleted_at IS NULL",
    )
    .bind(product_id)
    .fetch_optional(pool)
    .await
}

pub async fn create(
    pool: &MySqlPool,
    product_id: i64,
    quantity: i64,
    reserved: i64,
    threshold: i64,
) -> Result<Inventory, sqlx::Error> {
    let status = if quantity > 0 { "instock" } else { "out_of_stock" };

    sqlx::query(
        "INSERT INTO inventories (product_id, quantity, status, reserved, threshold) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(product_id)
    .bind(quantity)
    .bind(status)
    .bind(reserved)
    .bind(threshold)
    .execute(pool)
    .await?;

    find_by_product_id(pool, product_id)
        .await
        .map(|p| p.unwrap())
}

pub async fn update(
    pool: &MySqlPool,
    id: i64,
    quantity: Option<i64>,
    status: Option<&str>,
    reserved: Option<i64>,
    threshold: Option<i64>,
) -> Result<Option<Inventory>, sqlx::Error> {
    let mut sets: Vec<String> = Vec::new();
    if quantity.is_some() {
        sets.push("quantity = ?".to_string());
    }
    if status.is_some() {
        sets.push("status = ?".to_string());
    }
    if reserved.is_some() {
        sets.push("reserved = ?".to_string());
    }
    if threshold.is_some() {
        sets.push("threshold = ?".to_string());
    }

    if sets.is_empty() {
        return find_by_id(pool, id).await;
    }

    let sql = format!(
        "UPDATE inventories SET {} WHERE id = ? AND deleted_at IS NULL",
        sets.join(", ")
    );

    let mut q = sqlx::query(&sql);
    if let Some(v) = quantity {
        q = q.bind(v);
    }
    if let Some(v) = status {
        q = q.bind(v);
    }
    if let Some(v) = reserved {
        q = q.bind(v);
    }
    if let Some(v) = threshold {
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
        "UPDATE inventories SET deleted_at = NOW(3) WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn find_list(
    pool: &MySqlPool,
    status: Option<&str>,
    offset: u32,
    limit: u32,
) -> Result<Vec<Inventory>, sqlx::Error> {
    let mut where_clause = String::from("WHERE deleted_at IS NULL");

    if status.is_some() {
        where_clause.push_str(" AND status = ?");
    }

    let sql = format!(
        "SELECT id, product_id, quantity, status, reserved, threshold, created_at, updated_at, deleted_at \
         FROM inventories {} ORDER BY id DESC LIMIT ? OFFSET ?",
        where_clause
    );

    let mut q = sqlx::query_as::<_, Inventory>(&sql);
    if let Some(v) = status {
        q = q.bind(v);
    }
    q = q.bind(limit).bind(offset);

    q.fetch_all(pool).await
}

pub async fn count_list(
    pool: &MySqlPool,
    status: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let mut where_clause = String::from("WHERE deleted_at IS NULL");

    if status.is_some() {
        where_clause.push_str(" AND status = ?");
    }

    let sql = format!(
        "SELECT COUNT(*) FROM inventories {}",
        where_clause
    );

    let mut q = sqlx::query_scalar::<_, i64>(&sql);
    if let Some(v) = status {
        q = q.bind(v);
    }

    q.fetch_one(pool).await
}