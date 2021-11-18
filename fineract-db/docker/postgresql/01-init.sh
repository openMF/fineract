#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER $FINERACT_DB_USER WITH PASSWORD '$FINERACT_DB_PASS';
  CREATE DATABASE $FINERACT_TENANTS_DB_NAME;
  CREATE DATABASE $FINERACT_TENANT_DEFAULT_DB_NAME;
  GRANT ALL PRIVILEGES ON DATABASE $FINERACT_TENANTS_DB_NAME TO $FINERACT_DB_USER;
  GRANT ALL PRIVILEGES ON DATABASE $FINERACT_TENANT_DEFAULT_DB_NAME TO $FINERACT_DB_USER;
EOSQL