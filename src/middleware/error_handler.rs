use actix_web::http::StatusCode;

use crate::error::BizError;

pub fn map_biz_error_to_status(err: &BizError) -> StatusCode {
    match err.code {
        500 => StatusCode::INTERNAL_SERVER_ERROR,
        1004 => StatusCode::UNAUTHORIZED,
        2002 => StatusCode::FORBIDDEN,
        1008 => StatusCode::BAD_GATEWAY,
        1001 | 1005 | 1006 | 1010 | 2001 => StatusCode::NOT_FOUND,
        1007 | 1023 => StatusCode::CONFLICT,
        _ => StatusCode::BAD_REQUEST,
    }
}

pub fn set_global_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        log::error!("Unhandled panic: {panic_info}");
        prev(panic_info);
    }));
}
