use actix_web::{HttpResponse, web};
use sqlx::MySqlPool;

use crate::api::response;
use crate::dto::product::{CreateProductRequest, ProductListQuery, UpdateProductRequest};
use crate::service;

pub async fn create(
    pool: web::Data<MySqlPool>,
    body: web::Json<CreateProductRequest>,
) -> HttpResponse {
    match service::product::create_product(pool.get_ref(), body.into_inner()).await {
        Ok(product) => response::success(product),
        Err(err) => response::biz_error(&err),
    }
}

pub async fn get_by_id(pool: web::Data<MySqlPool>, path: web::Path<i64>) -> HttpResponse {
    let id = path.into_inner();
    match service::product::get_product(pool.get_ref(), id).await {
        Ok(product) => response::success(product),
        Err(err) => response::biz_error(&err),
    }
}

pub async fn update(
    pool: web::Data<MySqlPool>,
    path: web::Path<i64>,
    body: web::Json<UpdateProductRequest>,
) -> HttpResponse {
    let id = path.into_inner();
    match service::product::update_product(pool.get_ref(), id, body.into_inner()).await {
        Ok(product) => response::success(product),
        Err(err) => response::biz_error(&err),
    }
}

pub async fn delete(pool: web::Data<MySqlPool>, path: web::Path<i64>) -> HttpResponse {
    let id = path.into_inner();
    match service::product::delete_product(pool.get_ref(), id).await {
        Ok(()) => response::success("product deleted"),
        Err(err) => response::biz_error(&err),
    }
}

pub async fn list(pool: web::Data<MySqlPool>, query: web::Query<ProductListQuery>) -> HttpResponse {
    let q = query.into_inner();
    let page = q.page.unwrap_or(1).max(1);
    let page_size = q.page_size.unwrap_or(10).clamp(1, 100);

    match service::product::list_products(pool.get_ref(), q.keyword.as_deref(), page, page_size)
        .await
    {
        Ok(products) => response::success(products),
        Err(err) => response::biz_error(&err),
    }
}
