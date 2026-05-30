use actix_web::{HttpRequest, HttpResponse, web};
use sqlx::MySqlPool;

use crate::api::response;
use crate::dto::product::{CreateProductRequest, ProductListQuery, UpdateProductRequest};
use crate::error::ERR_INTERNAL_SERVER;
use crate::middleware::trace::get_trace_id;
use crate::service;
use crate::service::product_cache::{CachedItemResult, CachedListResult, ProductCache};

pub async fn create(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<ProductCache>,
    body: web::Json<CreateProductRequest>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let inner = body.into_inner();

    if inner.name.trim().is_empty() {
        return response::invalid_args("name is required");
    }
    if inner.price <= 0 {
        return response::invalid_args("price must be positive");
    }
    if inner.sku.trim().is_empty() {
        return response::invalid_args("sku is required");
    }

    match service::product::create_product(pool.get_ref(), cache.get_ref(), inner).await {
        Ok(product) => match trace_id {
            Some(tid) => response::success_with_trace(product, tid),
            None => response::success(product),
        },
        Err(err) if err == ERR_INTERNAL_SERVER => match trace_id {
            Some(tid) => response::sys_error_with_trace(err.message, tid),
            None => response::sys_error(err.message),
        },
        Err(err) => match trace_id {
            Some(tid) => response::biz_error_with_trace(&err, tid),
            None => response::biz_error(&err),
        },
    }
}

pub async fn get_by_id(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    path: web::Path<i64>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let id = path.into_inner();

    match service::product::get_product(pool.get_ref(), id).await {
        Ok(product) => match trace_id {
            Some(tid) => response::success_with_trace(product, tid),
            None => response::success(product),
        },
        Err(err) if err == ERR_INTERNAL_SERVER => match trace_id {
            Some(tid) => response::sys_error_with_trace(err.message, tid),
            None => response::sys_error(err.message),
        },
        Err(err) => match trace_id {
            Some(tid) => response::biz_error_with_trace(&err, tid),
            None => response::biz_error(&err),
        },
    }
}

pub async fn get_by_id_cached(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<ProductCache>,
    path: web::Path<i64>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let id = path.into_inner();

    match service::product::get_product_cached(pool.get_ref(), cache.get_ref(), id).await {
        Ok(CachedItemResult::Serialized(data_bytes)) => match trace_id {
            Some(tid) => response::success_from_bytes_with_trace(data_bytes, tid),
            None => response::success_from_bytes(data_bytes),
        },
        Ok(CachedItemResult::Fresh(product)) => match trace_id {
            Some(tid) => response::success_with_trace(product, tid),
            None => response::success(product),
        },
        Err(err) if err == ERR_INTERNAL_SERVER => match trace_id {
            Some(tid) => response::sys_error_with_trace(err.message, tid),
            None => response::sys_error(err.message),
        },
        Err(err) => match trace_id {
            Some(tid) => response::biz_error_with_trace(&err, tid),
            None => response::biz_error(&err),
        },
    }
}

pub async fn update(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<ProductCache>,
    path: web::Path<i64>,
    body: web::Json<UpdateProductRequest>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let id = path.into_inner();
    let inner = body.into_inner();

    if inner.name.is_none()
        && inner.description.is_none()
        && inner.price.is_none()
        && inner.sku.is_none()
    {
        return response::invalid_args("at least one field must be provided for update");
    }
    if let Some(price) = inner.price {
        if price <= 0 {
            return response::invalid_args("price must be positive");
        }
    }

    match service::product::update_product(pool.get_ref(), cache.get_ref(), id, inner).await {
        Ok(product) => match trace_id {
            Some(tid) => response::success_with_trace(product, tid),
            None => response::success(product),
        },
        Err(err) if err == ERR_INTERNAL_SERVER => match trace_id {
            Some(tid) => response::sys_error_with_trace(err.message, tid),
            None => response::sys_error(err.message),
        },
        Err(err) => match trace_id {
            Some(tid) => response::biz_error_with_trace(&err, tid),
            None => response::biz_error(&err),
        },
    }
}

pub async fn delete(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<ProductCache>,
    path: web::Path<i64>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let id = path.into_inner();

    match service::product::delete_product(pool.get_ref(), cache.get_ref(), id).await {
        Ok(()) => match trace_id {
            Some(tid) => response::success_with_trace("product deleted", tid),
            None => response::success("product deleted"),
        },
        Err(err) if err == ERR_INTERNAL_SERVER => match trace_id {
            Some(tid) => response::sys_error_with_trace(err.message, tid),
            None => response::sys_error(err.message),
        },
        Err(err) => match trace_id {
            Some(tid) => response::biz_error_with_trace(&err, tid),
            None => response::biz_error(&err),
        },
    }
}

pub async fn list(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    query: web::Query<ProductListQuery>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let q = query.into_inner();
    let page = q.page.unwrap_or(1).max(1);
    let page_size = q.page_size.unwrap_or(10).clamp(1, 100);

    match service::product::list_products(
        pool.get_ref(),
        q.keyword.as_deref(),
        page,
        page_size,
    )
    .await
    {
        Ok(products) => match trace_id {
            Some(tid) => response::success_with_trace(products, tid),
            None => response::success(products),
        },
        Err(err) if err == ERR_INTERNAL_SERVER => match trace_id {
            Some(tid) => response::sys_error_with_trace(err.message, tid),
            None => response::sys_error(err.message),
        },
        Err(err) => match trace_id {
            Some(tid) => response::biz_error_with_trace(&err, tid),
            None => response::biz_error(&err),
        },
    }
}

pub async fn list_cached(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<ProductCache>,
    query: web::Query<ProductListQuery>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let q = query.into_inner();
    let page = q.page.unwrap_or(1).max(1);
    let page_size = q.page_size.unwrap_or(10).clamp(1, 100);

    match service::product::list_products_cached(
        pool.get_ref(),
        cache.get_ref(),
        q.keyword.as_deref(),
        page,
        page_size,
    )
    .await
    {
        Ok(CachedListResult::Serialized(data_bytes)) => match trace_id {
            Some(tid) => response::success_from_bytes_with_trace(data_bytes, tid),
            None => response::success_from_bytes(data_bytes),
        },
        Ok(CachedListResult::Fresh(products)) => match trace_id {
            Some(tid) => response::success_with_trace(products, tid),
            None => response::success(products),
        },
        Err(err) if err == ERR_INTERNAL_SERVER => match trace_id {
            Some(tid) => response::sys_error_with_trace(err.message, tid),
            None => response::sys_error(err.message),
        },
        Err(err) => match trace_id {
            Some(tid) => response::biz_error_with_trace(&err, tid),
            None => response::biz_error(&err),
        },
    }
}

pub async fn warmup(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<ProductCache>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);

    match service::product::warmup_cache(pool.get_ref(), cache.get_ref()).await {
        Ok(count) => match trace_id {
            Some(tid) => response::success_with_trace(
                format!("cache warmed up with {} products", count),
                tid,
            ),
            None => response::success(format!("cache warmed up with {} products", count)),
        },
        Err(err) => match trace_id {
            Some(tid) => response::sys_error_with_trace(err.message, tid),
            None => response::sys_error(err.message),
        },
    }
}