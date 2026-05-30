use sqlx::MySqlPool;

use crate::dto::product::{
    CreateProductRequest, ProductListResponse, ProductResponse, UpdateProductRequest,
};
use crate::error::{BizError, ERR_DUPLICATE_SKU, ERR_INTERNAL_SERVER, ERR_PRODUCT_NOT_FOUND};
use crate::repository;
use crate::service::product_cache::ProductCache;

pub async fn get_product(pool: &MySqlPool, id: i64) -> Result<ProductResponse, BizError> {
    let product = repository::product::find_by_id(pool, id)
        .await
        .map_err(|e| {
            log::error!("[product_service] find_by_id error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    product
        .map(ProductResponse::from)
        .ok_or(ERR_PRODUCT_NOT_FOUND)
}

pub async fn get_product_cached(
    pool: &MySqlPool,
    cache: &ProductCache,
    id: i64,
) -> Result<ProductResponse, BizError> {
    cache.get_by_id(pool, id).await
}

pub async fn create_product(
    pool: &MySqlPool,
    cache: &ProductCache,
    req: CreateProductRequest,
) -> Result<ProductResponse, BizError> {
    let existing = repository::product::find_by_sku(pool, &req.sku)
        .await
        .map_err(|e| {
            log::error!("[product_service] find_by_sku error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    if existing.is_some() {
        return Err(ERR_DUPLICATE_SKU);
    }

    let product = repository::product::create(
        pool,
        &req.name,
        req.description.as_deref(),
        req.price,
        &req.sku,
    )
    .await
    .map_err(|e| {
        log::error!("[product_service] create error: {e}");
        ERR_INTERNAL_SERVER
    })?;

    cache.bloom_add(product.id);

    Ok(ProductResponse::from(product))
}

pub async fn update_product(
    pool: &MySqlPool,
    cache: &ProductCache,
    id: i64,
    req: UpdateProductRequest,
) -> Result<ProductResponse, BizError> {
    if let Some(ref sku) = req.sku {
        let existing = repository::product::find_by_sku(pool, sku)
            .await
            .map_err(|e| {
                log::error!("[product_service] find_by_sku error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        if let Some(p) = existing {
            if p.id != id {
                return Err(ERR_DUPLICATE_SKU);
            }
        }
    }

    let product = repository::product::update(
        pool,
        id,
        req.name.as_deref(),
        req.description.as_deref(),
        req.price,
        req.sku.as_deref(),
    )
    .await
    .map_err(|e| {
        log::error!("[product_service] update error: {e}");
        ERR_INTERNAL_SERVER
    })?;

    let response = product
        .map(ProductResponse::from)
        .ok_or(ERR_PRODUCT_NOT_FOUND)?;

    cache.delayed_double_delete(id).await;

    Ok(response)
}

pub async fn delete_product(
    pool: &MySqlPool,
    cache: &ProductCache,
    id: i64,
) -> Result<(), BizError> {
    let deleted = repository::product::soft_delete(pool, id)
        .await
        .map_err(|e| {
            log::error!("[product_service] soft_delete error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    if deleted {
        cache.delayed_double_delete(id).await;
        Ok(())
    } else {
        Err(ERR_PRODUCT_NOT_FOUND)
    }
}

pub async fn list_products(
    pool: &MySqlPool,
    keyword: Option<&str>,
    page: u32,
    page_size: u32,
) -> Result<ProductListResponse, BizError> {
    let offset = (page - 1) * page_size;

    let products = repository::product::find_list(pool, keyword, offset, page_size)
        .await
        .map_err(|e| {
            log::error!("[product_service] find_list error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    let total = repository::product::count_list(pool, keyword)
        .await
        .map_err(|e| {
            log::error!("[product_service] count_list error: {e}");
            ERR_INTERNAL_SERVER
        })?;

    Ok(ProductListResponse {
        list: products.into_iter().map(ProductResponse::from).collect(),
        total,
    })
}

pub async fn list_products_cached(
    pool: &MySqlPool,
    cache: &ProductCache,
    keyword: Option<&str>,
    page: u32,
    page_size: u32,
) -> Result<ProductListResponse, BizError> {
    cache.list(pool, keyword, page, page_size).await
}

pub async fn warmup_cache(pool: &MySqlPool, cache: &ProductCache) -> Result<i32, BizError> {
    cache.warmup(pool).await
}
