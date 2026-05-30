use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;

pub struct ConditionalLogger;

impl<S, B> Transform<S, ServiceRequest> for ConditionalLogger
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = ConditionalLoggerService<S>;
    type InitError = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Transform, Self::InitError>>>>;

    fn new_transform(&self, service: S) -> Self::Future {
        Box::pin(async { Ok(ConditionalLoggerService { service }) })
    }
}

pub struct ConditionalLoggerService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for ConditionalLoggerService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let should_log = needs_log(req.path());
        let start = if should_log { Some(Instant::now()) } else { None };
        let method = req.method().to_string();
        let path = req.path().to_string();

        let fut = self.service.call(req);

        Box::pin(async move {
            let res = fut.await?;
            if let Some(start) = start {
                let duration = start.elapsed();
                log::info!(
                    "{} {} {} {}ms",
                    method,
                    path,
                    res.status().as_u16(),
                    duration.as_millis()
                );
            }
            Ok(res)
        })
    }
}

fn needs_log(path: &str) -> bool {
    !path.contains("/cache/") && !path.ends_with("/cache") && !path.ends_with("/warmup")
}