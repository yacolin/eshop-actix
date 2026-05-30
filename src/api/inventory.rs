use actix_web::{HttpRequest, HttpResponse, web};
use sqlx::MySqlPool;

use crate::api::response;
use crate::dto::inventory::{CreateInventoryRequest, InventoryListQuery, UpdateInventoryRequest};
use crate::error::ERR_INTERNAL_SERVER;
use crate::middleware::trace::get_trace_id;
use crate::service;

pub async fn create(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    body: web::Json<CreateInventoryRequest>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let inner = body.into_inner();

    if inner.product_id <= 0 {
        return response::invalid_args("product_id must be positive");
    }

    match service::inventory::create_inventory(pool.get_ref(), inner).await {
        Ok(inventory) => match trace_id {
            Some(tid) => response::success_with_trace(inventory, tid),
            None => response::success(inventory),
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

    match service::inventory::get_inventory(pool.get_ref(), id).await {
        Ok(inventory) => match trace_id {
            Some(tid) => response::success_with_trace(inventory, tid),
            None => response::success(inventory),
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

pub async fn get_by_product_id(
    req: HttpRequest,
    pool: web::Data<MySqlPool>,
    path: web::Path<i64>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let product_id = path.into_inner();

    match service::inventory::get_inventory_by_product_id(pool.get_ref(), product_id).await {
        Ok(inventory) => match trace_id {
            Some(tid) => response::success_with_trace(inventory, tid),
            None => response::success(inventory),
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
    path: web::Path<i64>,
    body: web::Json<UpdateInventoryRequest>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let id = path.into_inner();
    let inner = body.into_inner();

    if inner.quantity.is_none()
        && inner.status.is_none()
        && inner.reserved.is_none()
        && inner.threshold.is_none()
    {
        return response::invalid_args("at least one field must be provided for update");
    }

    match service::inventory::update_inventory(pool.get_ref(), id, inner).await {
        Ok(inventory) => match trace_id {
            Some(tid) => response::success_with_trace(inventory, tid),
            None => response::success(inventory),
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
    path: web::Path<i64>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let id = path.into_inner();

    match service::inventory::delete_inventory(pool.get_ref(), id).await {
        Ok(()) => match trace_id {
            Some(tid) => response::success_with_trace("inventory deleted", tid),
            None => response::success("inventory deleted"),
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
    query: web::Query<InventoryListQuery>,
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let q = query.into_inner();
    let page = q.page.unwrap_or(1).max(1);
    let page_size = q.page_size.unwrap_or(10).clamp(1, 100);

    match service::inventory::list_inventories(
        pool.get_ref(),
        q.status.as_deref(),
        page,
        page_size,
    )
    .await
    {
        Ok(inventories) => match trace_id {
            Some(tid) => response::success_with_trace(inventories, tid),
            None => response::success(inventories),
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