use redis::aio::ConnectionManager;

const REDIS_POOL_SIZE: usize = 16;

pub async fn init_redis() -> Result<Vec<ConnectionManager>, Box<dyn std::error::Error>> {
    let redis_url =
        std::env::var("REDIS_URL").map_err(|_| "REDIS_URL not set, caching disabled")?;

    let client = redis::Client::open(redis_url.as_str())?;

    let mut connections = Vec::with_capacity(REDIS_POOL_SIZE);
    for i in 0..REDIS_POOL_SIZE {
        let conn = ConnectionManager::new(client.clone()).await?;
        log::info!("Redis connection {}/{} established", i + 1, REDIS_POOL_SIZE);
        connections.push(conn);
    }

    Ok(connections)
}