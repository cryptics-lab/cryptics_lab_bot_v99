# GitHub Secrets Setup Guide

This guide explains how to set up GitHub secrets for the CrypticsLabBot project.

## File Structure Context

- `.env` file is in the root directory
- Docker Compose and Dockerfiles are in the `docker/` folder
- The Docker Compose file references environment variables using `${VAR_NAME}` syntax

## Steps to Add GitHub Secrets

### 1. Navigate to GitHub Secrets

1. Go to: https://github.com/cryptics-lab/cryptics_lab_bot_v99
2. Click on **Settings** tab
3. In the left sidebar, click **Secrets and variables** → **Actions**
4. Click **New repository secret** for each secret below

### 2. Required Secrets

Add the following secrets exactly as shown:

#### Database Secrets
- **Name**: `DB_USER`
  **Value**: `postgres`
  
- **Name**: `DB_PASSWORD`
  **Value**: `postgres`
  
- **Name**: `DB_NAME`
  **Value**: `cryptics`
  
- **Name**: `DB_HOST`
  **Value**: `timescaledb`
  
- **Name**: `DB_PORT`
  **Value**: `5432`

#### Test Database Secrets (if using test environment)
- **Name**: `DB_TEST_PORT`
  **Value**: `5433`
  
- **Name**: `DB_TEST_NAME`
  **Value**: `cryptics_test`

#### PGAdmin Secrets
- **Name**: `PGADMIN_EMAIL`
  **Value**: `admin@cryptics.com`
  
- **Name**: `PGADMIN_PASSWORD`
  **Value**: `admin`

#### Grafana Secrets
- **Name**: `GRAFANA_USER`
  **Value**: `admin`
  
- **Name**: `GRAFANA_PASSWORD`
  **Value**: `admin`

#### Kafka Configuration
- **Name**: `ENABLE_KAFKA`
  **Value**: `true`
  
- **Name**: `KAFKA_BOOTSTRAP_SERVERS`
  **Value**: `localhost:9092`
  
- **Name**: `SCHEMA_REGISTRY_URL`
  **Value**: `http://localhost:8081`
  
- **Name**: `TOPIC_NAME`
  **Value**: `cryptics.thalex.instrument`

#### Thalex API Secrets (MOST IMPORTANT - Keep these secure!)
- **Name**: `THALEX_KID_TEST`
  **Value**: `K207709324`
  
- **Name**: `THALEX_KEY_TEST`
  **Value**: 
  ```
  -----BEGIN RSA PRIVATE KEY-----
  MIIEogIBAAKCAQEA1DSErVzk+TWZ0NnrNq7KbKAZDmVAte9kadYf/OcjsiTZmDn2OvLM/hk3E8DHejOyudgTN6vaSIh/nu0ixTEdBwnJs0y2f9+FpoE6PIQBfcGxVCjcfn4eioMwUXzWHxYCASv7L+aQDXmrmzAYBtv1UIdShbk8Xlvmp3CI7RM3DswcY2KCsu1Atng3IRq8uHaJ2ICOym30mhc7TEnj/bhGxnNisr4u7n25bYn626VpVr3cpPkhlXP18A7ubwCEoaCacGFA279Ic/d6J21WaeuhlBVq27Y4brdFGEuVS6/GRT5UsHQm2lKJS8tUZwOUXoNUTMS+5iZF1IgTCEMvnjcnowIDAQABAoIBAGqBdXoBntbJpUripSCL+AMvE4RbI3BtF6vbEbEAXbUis6eop8IMbQC3rSIX5saZvgFyxqpxcZxoDg25HXN1ZAlNS1PXk914VX8tawpGgu0YUyVXYNuH91Iz6ANuMZXmmNj3txnikbiBsbSxHc4LhgOF99AwGXGOlKTDYaYDt+WPeDKJRPUbyqZ0YC7uRlAT6w+v71LZT4YIFugd2xUBfZo8NRs0AIqf/0nkXjxkLL1TkhrfQ33MuCYE1sSJoU7Adit9UTN5rfUbYwS8XqW+rxWoNPVoUhpmJ09EnFj0KY4FJ5Tp5FRaNsgivkd7EMbgxk55zNmwjoRFWvfRCuKb4BkCgYEA8R0a5EEOs5Yjf92RzA+UNImuDBAOjnd2rVcuWSsjDZ55YlQWAEsrToTDHqNcxUJT1uEz7y9Di+BbXwdDLHhLB4L7p+3D6Avnuj7ndTiS3m0zlYuHJt5uOpW1MJea/AYZZEgQw1oxDvpirqTJAUrVcW25yD2e6o96omAFu/5hIDUCgYEA4U6Ap8IEyEYTzWFS7uniZPhQ/y9D+itgyFup3XAuLJLQDKfWf+qxpBcoQNp9akfbKmkZAdN/Py4Z5eSGr9jwi76CaWjNrCx/OspGpC6yr604D9/EStDvZKO6MIPqzjrjrPAj3wr+x+7pyNGK7hbpoCmv2vqUahro2ekcFM8gU3cCgYA+mVbdAgMGHynSTb1HpJfO2BwG57hPxrZaJLNU1T1BkO5k1/3qT5nLxe4+qx0v3ZuUw11PcQ7xZutyDZmkFwsrxRb/L1zYhxa/pQtExM5kzydAYTxSnBc0QKANIJ9NZWv8PDWV8nmgMOOkAgZpvnsR+vHsCguTTHMDazix6UZgLQKBgBVufDp48C+EyvlsWpEzWj+0hy/e9R5k5e3pGj1gIGRJCtVJWdQNJRywvzl8DxX/A9AC80gDMEV4QyplFJLBlhAU7R7Nw1KvYOLcvt97ObAQUBbieC+NtFfkYx+eTMWVQHcA2MisqROnnEFn/UkskMiVbo7r6xY0vRWNYQhxs2d9AoGAOtb1YLS0YId2dtB9Syygq6L+nqIsBQhvfmhYB7V4AkC0IuWcov3jUTz6vAaNMVqlYs+oqz1S+F1B687XwaSnq4IirxutrmnKKiObJdKk+9imeKBlgxeiEjmzfGhF6oAlmBvuyzgBiTGRAFj20m+rzRvYi4xfjdbVmDtE1CpJM+Y=
  -----END RSA PRIVATE KEY-----
  ```
  **Note**: Copy the entire private key including the BEGIN and END lines

## 3. Creating a GitHub Actions Workflow

After adding secrets, create a workflow file that uses them:

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Create .env file
        run: |
          cat << EOF > .env
          # Database Configuration
          DB_HOST=${{ secrets.DB_HOST }}
          DB_PORT=${{ secrets.DB_PORT }}
          DB_USER=${{ secrets.DB_USER }}
          DB_PASSWORD=${{ secrets.DB_PASSWORD }}
          DB_NAME=${{ secrets.DB_NAME }}
          DB_TEST_PORT=${{ secrets.DB_TEST_PORT }}
          DB_TEST_NAME=${{ secrets.DB_TEST_NAME }}
          
          # PGAdmin
          PGADMIN_EMAIL=${{ secrets.PGADMIN_EMAIL }}
          PGADMIN_PASSWORD=${{ secrets.PGADMIN_PASSWORD }}
          
          # Grafana
          GRAFANA_USER=${{ secrets.GRAFANA_USER }}
          GRAFANA_PASSWORD=${{ secrets.GRAFANA_PASSWORD }}
          
          # Kafka
          ENABLE_KAFKA=${{ secrets.ENABLE_KAFKA }}
          KAFKA_BOOTSTRAP_SERVERS=${{ secrets.KAFKA_BOOTSTRAP_SERVERS }}
          SCHEMA_REGISTRY_URL=${{ secrets.SCHEMA_REGISTRY_URL }}
          TOPIC_NAME=${{ secrets.TOPIC_NAME }}
          
          # Thalex API
          THALEX_KID_TEST=${{ secrets.THALEX_KID_TEST }}
          THALEX_KEY_TEST="${{ secrets.THALEX_KEY_TEST }}"
          EOF
      
      - name: Build and Deploy
        run: |
          cd docker
          docker-compose up -d
```

## 4. Using Secrets in Different Contexts

### For Docker Compose (Local Development)
The Docker Compose file in `docker/docker-compose.yml` references variables from the `.env` file in the root directory using `${VAR_NAME}` syntax.

### For GitHub Actions
GitHub Actions can access secrets using `${{ secrets.SECRET_NAME }}` syntax.

### For Production Deployment
Never commit `.env` files. Always use:
- GitHub Secrets for GitHub Actions
- Environment variables for cloud deployments
- Secret management services (AWS Secrets Manager, HashiCorp Vault, etc.)

## Security Best Practices

1. **Never commit secrets** to version control
2. **Rotate secrets regularly** especially API keys
3. **Use different secrets** for development, staging, and production
4. **Limit secret access** to only necessary workflows/environments
5. **Monitor secret usage** through GitHub audit logs

## Verifying Your Setup

After adding all secrets:
1. Go to Settings → Secrets and variables → Actions
2. You should see all secrets listed (values will be hidden)
3. Run a test workflow to ensure secrets are accessible

## Next Steps

Once secrets are configured:
1. Remove `.env` from version control
2. Add `.env` to `.gitignore`
3. Update documentation to reference this guide
4. Clean git history to remove any committed secrets