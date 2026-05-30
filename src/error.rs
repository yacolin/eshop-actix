use std::fmt;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BizError {
    pub code: i32,
    pub message: &'static str,
}

impl BizError {
    pub const fn new(code: i32, message: &'static str) -> Self {
        BizError { code, message }
    }
}

impl fmt::Display for BizError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "code: {}, message: {}", self.code, self.message)
    }
}

// ==================== Domain: General (1001-1999) ====================
pub const ERR_PRODUCT_NOT_FOUND: BizError = BizError::new(1001, "product not found");
// pub const ERR_INVALID_PARAMS: BizError = BizError::new(1002, "invalid parameters");
// pub const ERR_PAGINATION_QUERY: BizError = BizError::new(1003, "invalid pagination query");
pub const ERR_UNAUTHORIZED: BizError = BizError::new(1004, "unauthorized");
// pub const ERR_USER_NOT_FOUND: BizError = BizError::new(1005, "user not found");
// pub const ERR_ORDER_NOT_FOUND: BizError = BizError::new(1006, "order not found");
// pub const ERR_DUPLICATE_ORDER: BizError = BizError::new(1007, "duplicate order");
pub const ERR_PAYMENT_FAILED: BizError = BizError::new(1008, "payment failed");

// pub const ERR_INVALID_CREDENTIALS: BizError = BizError::new(1009, "invalid credentials");
pub const ERR_NOT_FOUND: BizError = BizError::new(1010, "resource not found");
// pub const ERR_ACCOUNT_DISABLED: BizError = BizError::new(1011, "account disabled");
// pub const ERR_WECHAT_CLIENT_NOT_CONFIGURED: BizError =
//     BizError::new(1012, "wechat client not configured");
// pub const ERR_USERNAME_ALREADY_EXISTS: BizError = BizError::new(1013, "username already exists");
// pub const ERR_UNSUPPORTED_PROVIDER: BizError = BizError::new(1014, "unsupported provider");
// pub const ERR_IDENTITY_ALREADY_BOUND: BizError = BizError::new(1015, "identity already bound");
// pub const ERR_INVALID_TOKEN: BizError = BizError::new(1016, "invalid token");
// pub const ERR_TOKEN_REVOKED: BizError = BizError::new(1017, "token revoked");

// pub const ERR_GENERATE_ACCESS_TOKEN: BizError =
//     BizError::new(1018, "generate access token failed");
// pub const ERR_GENERATE_REFRESH_TOKEN: BizError =
//     BizError::new(1019, "generate refresh token failed");
// pub const ERR_SAVE_REFRESH_TOKEN: BizError = BizError::new(1020, "save refresh token failed");
// pub const ERR_UNEXPECTED_SIGNING_METHOD: BizError =
//     BizError::new(1021, "unexpected signing method");
// pub const ERR_PARSE_TOKEN: BizError = BizError::new(1022, "parse token failed");
// pub const ERR_DUPLICATE_SKU: BizError = BizError::new(1023, "duplicate sku");

// ==================== Domain: Permission (2001-2999) ====================
// pub const ERR_PERMISSION_NOT_FOUND: BizError = BizError::new(2001, "permission not found");
// pub const ERR_INSUFFICIENT_PERMISSIONS: BizError =
//     BizError::new(2002, "insufficient permissions");
// pub const ERR_CANNOT_MODIFY_SYSTEM_ROLE: BizError =
//     BizError::new(2003, "cannot modify system role");
// pub const ERR_CANNOT_DELETE_SYSTEM_ROLE: BizError =
//     BizError::new(2004, "cannot delete system role");