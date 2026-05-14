#!/bin/bash

# Configuration
VAULT_ADDR="https://vault.p.zacharie.org"
VAULT_PATH="ai/mnemosyne"

echo "🔐 Setting up Vault secrets for Mnemosyne at $VAULT_ADDR"

# Check if logged in
vault status > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "❌ Not logged into Vault. Please run 'vault login' first."
    exit 1
fi

# Prompt for values if not provided
if [ -z "$DATABASE_URL" ]; then
    read -p "Enter DATABASE_URL: " DATABASE_URL
fi

if [ -z "$LITELLM_API_KEY" ]; then
    read -p "Enter LITELLM_API_KEY: " LITELLM_API_KEY
fi

# Put secrets in Vault
vault kv put $VAULT_PATH \
    DATABASE_URL="$DATABASE_URL" \
    LITELLM_API_KEY="$LITELLM_API_KEY"

echo "✅ Secrets successfully stored in $VAULT_PATH"
