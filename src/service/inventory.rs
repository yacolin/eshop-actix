use sqlx::MySqlPool;

use crate::dto::inventory::{
    CreateInventoryRequest, InventoryListResponse, InventoryResponse, UpdateInventoryRequest,
};
use crate::error::{BizError, ERR_INTERNAL_SERVER, ERR_INVENTORY_NOT_FOUND, ERR_DUPLICATE_INVENTORY};
use crate::repository;

pub async fn get_inventory(pool: &MySqlPool, id: i64) -> Result<InventoryResponse, BizError> {
    let inventory = repository::inventory::find_by_id(pool, id)
        .await
        .map_err(|e| {
            log::error!("[inventory_service] find_by_id error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    inventory
        .map(InventoryResponse::from)
        .ok_or(ERR_INVENTORY_NOT_FOUND)
}

pub async fn get_inventory_by_product_id(
    pool: &MySqlPool,
    product_id: i64,
) -> Result<InventoryResponse, BizError> {
    let inventory = repository::inventory::find_by_product_id(pool, product_id)
        .await
        .map_err(|e| {
            log::error!("[inventory_service] find_by_product_id error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    inventory
        .map(InventoryResponse::from)
        .ok_or(ERR_INVENTORY_NOT_FOUND)
}

pub async fn create_inventory(
    pool: &MySqlPool,
    req: CreateInventoryRequest,
) -> Result<InventoryResponse, BizError> {
    if req.quantity < 0 {
        return Err(BizError::new(1002, "quantity must be non-negative"));
    }

    let existing = repository::inventory::find_by_product_id(pool, req.product_id)
        .await
        .map_err(|e| {
            log::error!("[inventory_service] find_by_product_id error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    if existing.is_some() {
        return Err(ERR_DUPLICATE_INVENTORY);
    }

    let inventory = repository::inventory::create(
        pool,
        req.product_id,
        req.quantity,
        req.reserved.unwrap_or(0),
        req.threshold.unwrap_or(10),
    )
    .await
    .map_err(|e| {
        log::error!("[inventory_service] create error: {e}");
        ERR_INTERNAL_SERVER
    })?;

    Ok(InventoryResponse::from(inventory))
}

pub async fn update_inventory(
    pool: &MySqlPool,
    id: i64,
    req: UpdateInventoryRequest,
) -> Result<InventoryResponse, BizError> {
    if let Some(quantity) = req.quantity {
        if quantity < 0 {
            return Err(BizError::new(1002, "quantity must be non-negative"));
        }
    }

    let computed_status = req.status.or_else(|| {
        req.quantity.map(|q| {
            if q > 0 { "instock".to_string() } else { "out_of_stock".to_string() }
        })
    });

    let inventory = repository::inventory::update(
        pool,
        id,
        req.quantity,
        computed_status.as_deref(),
        req.reserved,
        req.threshold,
    )
    .await
    .map_err(|e| {
        log::error!("[inventory_service] update error: {e}");
        ERR_INTERNAL_SERVER
    })?;

    inventory
        .map(InventoryResponse::from)
        .ok_or(ERR_INVENTORY_NOT_FOUND)
}

pub async fn delete_inventory(pool: &MySqlPool, id: i64) -> Result<(), BizError> {
    let deleted = repository::inventory::soft_delete(pool, id)
        .await
        .map_err(|e| {
            log::error!("[inventory_service] soft_delete error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    if deleted {
        Ok(())
    } else {
        Err(ERR_INVENTORY_NOT_FOUND)
    }
}

pub async fn list_inventories(
    pool: &MySqlPool,
    status: Option<&str>,
    page: u32,
    page_size: u32,
) -> Result<InventoryListResponse, BizError> {
    let offset = (page - 1) * page_size;

    let inventories = repository::inventory::find_list(pool, status, offset, page_size)
        .await
        .map_err(|e| {
            log::error!("[inventory_service] find_list error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    let total = repository::inventory::count_list(pool, status)
        .await
        .map_err(|e| {
            log::error!("[inventory_service] count_list error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    Ok(InventoryListResponse {
        list: inventories.into_iter().map(InventoryResponse::from).collect(),
        total,
    })
}