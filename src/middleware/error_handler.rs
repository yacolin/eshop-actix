use std::future::{ready, Ready};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;

use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::StatusCode;
use actix_web::{Error, HttpMessage};
use futures::FutureExt;

use crate::error::BizError;

// ============================================================
// ErrorHandler Middleware
// ============================================================

pub struct ErrorHandler;

impl<S, B> Transform<S, ServiceRequest> for ErrorHandler
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = ErrorHandlerService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ErrorHandlerService { service }))
    }
}

pub struct ErrorHandlerService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for ErrorHandlerService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let trace_id = req
            .extensions()
            .get::<super::trace::TraceId>()
            .map(|t| t.0.clone())
            .unwrap_or_default();

        let fut = AssertUnwindSafe(self.service.call(req)).catch_unwind();

        Box::pin(async move {
            match fut.await {
                Ok(Ok(res)) => {
                    Ok(res)
                }
                Ok(Err(err)) => {
                    log::error!("[trace: {trace_id}] Service error: {err}");
                    Err(err)
                }
                Err(panic_err) => {
                    let msg = panic_err
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| panic_err.downcast_ref::<String>().map(|s| s.as_str()))
                        .unwrap_or("unknown panic");
                    log::error!("[trace: {trace_id}] Handler panicked: {msg}");
                    Err(actix_web::error::InternalError::new(
                        format!("internal server error (trace: {trace_id})"),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                    .into())
                }
            }
        })
    }
}

// ============================================================
// BizError → HTTP Status 映射（对应 Go errorhandler.go 中 mapBizErrorToStatus）
// ============================================================

pub fn map_biz_error_to_status(err: &BizError) -> StatusCode {
    match err.code {
        1004 => StatusCode::UNAUTHORIZED,
        2002 => StatusCode::FORBIDDEN,
        1008 => StatusCode::BAD_GATEWAY,
        1001 | 1005 | 1006 | 1010 | 2001 => StatusCode::NOT_FOUND,
        1007 | 1023 => StatusCode::CONFLICT,
        _ => StatusCode::BAD_REQUEST,
    }
}

// ============================================================
// 全局 panic hook，记录未捕获的 panic
// ============================================================

pub fn set_global_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        log::error!("Unhandled panic: {panic_info}");
        prev(panic_info);
    }));
}