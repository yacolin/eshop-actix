use actix_web::middleware::Logger;
use actix_web::{App, HttpServer, Responder, get, post, web};
use serde::Serialize;

mod api;
mod cache;
mod db;
mod dto;
mod error;
mod middleware;
mod models;
mod repository;
mod service;

use api::response;
use middleware::error_handler::{ErrorHandler, set_global_panic_hook};
use middleware::trace::{TraceMiddleware, get_trace_id};
use service::product_cache::ProductCache;

#[get("/")]
async fn hello() -> impl Responder {
    response::success("Hello world!")
}

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    response::success(req_body)
}

async fn manual_hello() -> impl Responder {
    response::success("Hey there!")
}

#[derive(Serialize)]
struct DbStatusData {
    status: String,
}

#[get("/db_status")]
async fn db_status(pool: web::Data<sqlx::MySqlPool>) -> impl Responder {
    match sqlx::query("SELECT 1").execute(pool.get_ref()).await {
        Ok(_) => response::success(DbStatusData {
            status: "connected".to_string(),
        }),
        Err(_e) => response::biz_error(&error::ERR_PAYMENT_FAILED),
    }
}

#[get("/products/{id}")]
async fn get_product(path: web::Path<i32>) -> impl Responder {
    let _id = path.into_inner();

    response::biz_error(&error::ERR_PRODUCT_NOT_FOUND)
}

#[get("/unauthorized")]
async fn unauthorized() -> impl Responder {
    response::biz_error(&error::ERR_UNAUTHORIZED)
}

#[get("/trace")]
async fn trace_demo(req: actix_web::HttpRequest) -> impl Responder {
    let trace_id = get_trace_id(&req).unwrap_or_default();
    response::success_with_trace("this request has a trace id", trace_id)
}

async fn not_found() -> impl Responder {
    response::biz_error(&error::ERR_NOT_FOUND)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    set_global_panic_hook();

    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let pool = db::init_pool().await;

    let cache = match cache::init_redis().await {
        Ok(conns) => {
            log::info!("Redis connection pool established ({} connections)", conns.len());
            ProductCache::new(Some(conns))
        }
        Err(e) => {
            log::warn!("Redis not available, caching disabled: {e}");
            ProductCache::new(None)
        }
    };
    let cache_data = web::Data::new(cache);

    log::info!("Starting server at http://127.0.0.1:8080");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(cache_data.clone())
            .wrap(TraceMiddleware)
            .wrap(ErrorHandler)
            .wrap(Logger::new("%t | %r | %s | %b bytes | %Dms"))
            .service(hello)
            .service(echo)
            .service(db_status)
            .service(get_product)
            .service(unauthorized)
            .service(trace_demo)
            .route("/hey", web::get().to(manual_hello))
            .service(
                web::scope("/api/v1/products")
                    .route("", web::post().to(api::product::create))
                    .route("", web::get().to(api::product::list))
                    .route("/cache", web::get().to(api::product::list_cached))
                    .route("/cache/{id}", web::get().to(api::product::get_by_id_cached))
                    .route("/warmup", web::post().to(api::product::warmup))
                    .route("/{id}", web::get().to(api::product::get_by_id))
                    .route("/{id}", web::put().to(api::product::update))
                    .route("/{id}", web::delete().to(api::product::delete)),
            )
            .default_service(web::to(not_found))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
