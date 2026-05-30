use actix_web::HttpResponse;
use actix_web::http::StatusCode;
use serde::Serialize;

use crate::error::BizError;

#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

pub fn success<T: Serialize>(data: T) -> HttpResponse {
    HttpResponse::Ok().json(ApiResponse {
        code: 0,
        message: "success".to_string(),
        data: Some(data),
        trace_id: None,
    })
}

pub fn success_with_trace<T: Serialize>(data: T, trace_id: String) -> HttpResponse {
    HttpResponse::Ok().json(ApiResponse {
        code: 0,
        message: "success".to_string(),
        data: Some(data),
        trace_id: Some(trace_id),
    })
}

pub fn biz_error(err: &BizError) -> HttpResponse {
    let status = crate::middleware::error_handler::map_biz_error_to_status(err);
    biz_error_with_status(err, status)
}

pub fn biz_error_with_status(err: &BizError, status: StatusCode) -> HttpResponse {
    HttpResponse::build(status).json(ApiResponse::<()> {
        code: err.code,
        message: err.message.to_string(),
        data: None,
        trace_id: None,
    })
}

pub fn biz_error_with_trace(err: &BizError, trace_id: String) -> HttpResponse {
    let status = crate::middleware::error_handler::map_biz_error_to_status(err);
    HttpResponse::build(status).json(ApiResponse::<()> {
        code: err.code,
        message: err.message.to_string(),
        data: None,
        trace_id: Some(trace_id),
    })
}
#[allow(dead_code)]
pub fn invalid_args(message: &str) -> HttpResponse {
    HttpResponse::UnprocessableEntity().json(ApiResponse::<()> {
        code: 1002,
        message: message.to_string(),
        data: None,
        trace_id: None,
    })
}

pub fn sys_error(err: impl std::fmt::Display) -> HttpResponse {
    log::error!("Internal server error: {err}");
    HttpResponse::InternalServerError().json(ApiResponse::<()> {
        code: 500,
        message: err.to_string(),
        data: None,
        trace_id: None,
    })
}
#[allow(dead_code)]
pub fn sys_error_with_trace(err: impl std::fmt::Display, trace_id: String) -> HttpResponse {
    log::error!("[trace: {trace_id}] Internal server error: {err}");
    HttpResponse::InternalServerError().json(ApiResponse::<()> {
        code: 500,
        message: err.to_string(),
        data: None,
        trace_id: Some(trace_id),
    })
}
