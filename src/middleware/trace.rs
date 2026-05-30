use std::future::{ready, Ready};

use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, HttpMessage};
use uuid::Uuid;

pub struct TraceMiddleware;

impl<S, B> Transform<S, ServiceRequest> for TraceMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = TraceMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(TraceMiddlewareService { service }))
    }
}

pub struct TraceMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for TraceMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = S::Future;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let trace_id = Uuid::new_v4().to_string();
        req.extensions_mut().insert(TraceId(trace_id));
        self.service.call(req)
    }
}

#[derive(Debug, Clone)]
pub struct TraceId(pub String);

pub fn get_trace_id(req: &actix_web::HttpRequest) -> Option<String> {
    req.extensions()
        .get::<TraceId>()
        .map(|t| t.0.clone())
}