use sqlx::PgPool;
use uuid::Uuid;
use chrono::Utc;
use dotenvy::dotenv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;

    let user_id = Uuid::new_v4();
    let username = "admin";
    let password_hash = "password"; // In a real app, hash this with bcrypt
    let email = "admin@mnemosyne.io";

    println!("🌱 Seeding admin user...");

    sqlx::query(
        "INSERT INTO users (id, username, password_hash, email, created_at) 
         VALUES ($1, $2, $3, $4, $5) 
         ON CONFLICT (username) DO NOTHING"
    )
    .bind(user_id)
    .bind(username)
    .bind(password_hash)
    .bind(email)
    .bind(Utc::now())
    .execute(&pool)
    .await?;

    println!("✅ User 'admin' created (password: 'password')");

    Ok(())
}
