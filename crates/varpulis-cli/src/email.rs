//! SMTP email sender for verification emails.

use std::sync::Arc;

use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

/// SMTP configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub from: String,
    pub frontend_url: String,
}

impl SmtpConfig {
    /// Build config from environment variables.
    /// Returns None if `SMTP_HOST` is not set.
    pub fn from_env() -> Option<Self> {
        let host = std::env::var("SMTP_HOST").ok()?;
        let port = std::env::var("SMTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(587);
        let username = std::env::var("SMTP_USERNAME").ok();
        let password = std::env::var("SMTP_PASSWORD").ok();
        let from = std::env::var("SMTP_FROM").unwrap_or_else(|_| "noreply@varpulis.com".into());
        let frontend_url =
            std::env::var("FRONTEND_URL").unwrap_or_else(|_| "http://localhost:5173".into());

        Some(Self {
            host,
            port,
            username,
            password,
            from,
            frontend_url,
        })
    }
}

/// Email sender backed by SMTP.
#[derive(Debug)]
pub struct EmailSender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: String,
    frontend_url: String,
}

pub type SharedEmailSender = Arc<EmailSender>;

impl EmailSender {
    /// Create a new sender from SMTP configuration.
    pub fn new(config: SmtpConfig) -> Result<Self, lettre::transport::smtp::Error> {
        let mut builder =
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.host)?.port(config.port);

        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            builder = builder.credentials(Credentials::new(user.clone(), pass.clone()));
        }

        // For local dev (MailPit) or explicit opt-in, disable TLS certificate verification
        if config.port == 1025 || std::env::var("VARPULIS_SMTP_DANGEROUS").is_ok() {
            tracing::warn!(
                "SMTP: TLS certificate verification disabled for {}:{} — \
                 do not use in production",
                config.host,
                config.port
            );
            builder = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&config.host)
                .port(config.port);
        }

        Ok(Self {
            transport: builder.build(),
            from: config.from,
            frontend_url: config.frontend_url,
        })
    }

    /// Send a verification email with the token link.
    pub async fn send_verification_email(
        &self,
        to_email: &str,
        to_name: &str,
        token: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let verify_url = format!("{}/verify-email?token={}", self.frontend_url, token);

        let body = format!(
            "Hello {to_name},\n\n\
             Please verify your email address by clicking the link below:\n\n\
             {verify_url}\n\n\
             This link expires in 24 hours.\n\n\
             If you did not create an account, please ignore this email.\n\n\
             - The Varpulis Team"
        );

        let email = Message::builder()
            .from(self.from.parse()?)
            .to(format!("{to_name} <{to_email}>").parse()?)
            .subject("Verify your Varpulis account")
            .header(ContentType::TEXT_PLAIN)
            .body(body)?;

        self.transport.send(email).await?;
        Ok(())
    }

    /// Send a campaign/announcement email.
    pub async fn send_campaign_email(
        &self,
        to_email: &str,
        subject: &str,
        body: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let email = Message::builder()
            .from(self.from.parse()?)
            .to(to_email.parse()?)
            .subject(subject)
            .header(ContentType::TEXT_PLAIN)
            .body(body.to_string())?;

        self.transport.send(email).await?;
        Ok(())
    }
}

/// Generate a 64-character random alphanumeric verification token.
pub fn generate_verification_token() -> String {
    use rand::RngExt;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..64)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}
