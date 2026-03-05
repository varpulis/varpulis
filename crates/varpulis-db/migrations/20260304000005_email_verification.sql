-- Email verification support for self-service signup
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_token TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_expires_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_verification_token
    ON users(verification_token) WHERE verification_token IS NOT NULL;

-- Clear duplicate emails before creating unique index (keep first occurrence)
UPDATE users SET email = ''
WHERE id IN (
    SELECT id FROM (
        SELECT id, ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at) AS rn
        FROM users
        WHERE email != '' AND email IS NOT NULL
    ) sub WHERE rn > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique
    ON users(email) WHERE email != '' AND email IS NOT NULL;

-- Existing users (admin-created, OAuth) are already verified
UPDATE users SET email_verified = true WHERE email_verified = false;
