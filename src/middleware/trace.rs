use actix_web::HttpMessage;

#[derive(Debug, Clone)]
pub struct TraceId(pub String);

pub fn get_trace_id(req: &actix_web::HttpRequest) -> Option<String> {
    req.extensions()
        .get::<TraceId>()
        .map(|t| t.0.clone())
}
