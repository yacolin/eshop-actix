use redis::aio::ConnectionManager;

pub async fn init_redis() -> Result<ConnectionManager, Box<dyn std::error::Error>> {
    let redis_url =
        std::env::var("REDIS_URL").map_err(|_| "REDIS_URL not set, caching disabled")?;
    let client = redis::Client::open(redis_url.as_str())?;
    let conn = ConnectionManager::new(client).await?;
    Ok(conn)
}