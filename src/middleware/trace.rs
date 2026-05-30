use std::future::{ready, Ready};

use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, HttpMessage};
use uuid::Uuid;

pub struct ConditionalTrace;

impl<S, B> Transform<S, ServiceRequest> for ConditionalTrace
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = ConditionalTraceService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ConditionalTraceService { service }))
    }
}

pub struct ConditionalTraceService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for ConditionalTraceService<S>
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
        if needs_trace_id(req.path()) {
            let trace_id = Uuid::new_v4().to_string();
            req.extensions_mut().insert(TraceId(trace_id));
        }
        self.service.call(req)
    }
}

fn needs_trace_id(path: &str) -> bool {
    !path.contains("/cache/") && !path.ends_with("/cache") && !path.ends_with("/warmup")
}

#[derive(Debug, Clone)]
pub struct TraceId(pub String);

pub fn get_trace_id(req: &actix_web::HttpRequest) -> Option<String> {
    req.extensions()
        .get::<TraceId>()
        .map(|t| t.0.clone())
}