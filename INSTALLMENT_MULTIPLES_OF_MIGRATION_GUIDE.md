# InstallmentAmountInMultiplesOf Migration Guide

## Overview

Four-step migration to move `installmentAmountInMultiplesOf` from `m_product_loan` to `m_loan` table.

**Branch**: `resolve-InstallmentAmountInMultiplesOf-issue`

**Steps**:
- Step 1: Commit `950a8b6347` - Schema + backward compatibility
- Step 2: Commit `d38db637f8` - Data migration
- Step 3: Commit `8aac96c4a8` - Cleanup
- Step 4: Fix Liquibase `databasechangelog` table to be compatible with migrations from develop branch

**Migration Files**: `fineract-loan/src/main/resources/db/changelog/tenant/module/loan/parts/`
- `1029_step_1_migration_installment_amount_in_multiples_of.xml` (changesets 1029-step-1-1, 1029-step-1-2)
- `1029_step_2_migration_installment_amount_in_multiples_of.xml` (changeset 1029-step-2)
- `1029_step_3_migration_installment_amount_in_multiples_of.xml` (changeset 1029-step-3)

**Key Features**:
- GENERATED virtual column for backward compatibility
- Batch processing (10,000 rows) with SKIP LOCKED
- Selective updates (only NULL or different values)
- Idempotent and resumable

---

## Step 1: Schema + Backward Compatibility
**Commit**: `950a8b6347`

**Database** (changesets 1029-step-1-1, 1029-step-1-2):
```sql
-- Add column to m_loan
ALTER TABLE m_loan
ADD COLUMN installment_amount_in_multiples_of DECIMAL(19,6) DEFAULT NULL;

-- Rename column in m_product_loan
ALTER TABLE m_product_loan
RENAME COLUMN instalment_amount_in_multiples_of TO installment_amount_in_multiples_of;

-- Create virtual column for backward compatibility
ALTER TABLE m_product_loan
ADD COLUMN instalment_amount_in_multiples_of DECIMAL(19,6)
    GENERATED ALWAYS AS (installment_amount_in_multiples_of) STORED;
```

**Application Code**:
- Move `installmentAmountInMultiplesOf` from `LoanProduct` to `LoanProductRelatedDetail`
- Add `Loan.getInstallmentAmountInMultiplesOf()` with fallback to product value
- Update all usages to call `loan.getInstallmentAmountInMultiplesOf()`

**Limitation**: Cannot create loan products from old app instances (virtual column is read-only).

---

## Step 2: Data Migration
**Commit**: `d38db637f8`

**Database** (changeset 1029-step-2, `runInTransaction="false"`, `context="postgresql"`):
```sql
DO $$
DECLARE
    v_rows_updated INTEGER;
    v_batch_count INTEGER := 0;
    v_total_updated BIGINT := 0;
    v_start_time TIMESTAMP := NOW();
    v_duration NUMERIC;
BEGIN
    RAISE NOTICE 'Starting batch update at % with batch size: 10000', v_start_time;

    LOOP
        WITH batch AS (
            SELECT l.id, lp.installment_amount_in_multiples_of
            FROM m_loan l
            INNER JOIN m_product_loan lp ON lp.id = l.product_id
            WHERE (l.installment_amount_in_multiples_of IS NULL
                   OR l.installment_amount_in_multiples_of IS DISTINCT FROM lp.installment_amount_in_multiples_of)
              AND lp.installment_amount_in_multiples_of IS NOT NULL
            ORDER BY l.id
            LIMIT 10000
            FOR UPDATE OF l SKIP LOCKED
        )
        UPDATE m_loan
        SET installment_amount_in_multiples_of = batch.installment_amount_in_multiples_of
        FROM batch
        WHERE m_loan.id = batch.id;

        GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
        v_batch_count := v_batch_count + 1;
        v_total_updated := v_total_updated + v_rows_updated;

        RAISE NOTICE 'Batch %: Updated % rows. Total: %', v_batch_count, v_rows_updated, v_total_updated;

        EXIT WHEN v_rows_updated = 0;
        COMMIT;

        PERFORM pg_sleep(0.1);
    END LOOP;

    v_duration := EXTRACT(EPOCH FROM (NOW() - v_start_time));

    RAISE NOTICE 'Completed at %. Total rows updated: %. Duration: % seconds', NOW(), v_total_updated, v_duration;
END $$;
```

**Key Features**:
- Batch size: 10,000 rows
- `SKIP LOCKED`: Non-blocking, handles concurrent updates
- Selective: Only NULL or different values
- Throttling: 100ms pause between batches
- Resumable and idempotent

**Benchmark**: 1M loans in 139 seconds (local PostgreSQL, version 16.4).

---

## Step 3: Cleanup
**Commit**: `8aac96c4a8`

**Database** (changeset 1029-step-3):
```xml
<changeSet author="fineract" id="1029-step-3" objectQuotingStrategy="QUOTE_ALL_OBJECTS">
    <dropColumn tableName="m_product_loan" columnName="instalment_amount_in_multiples_of"/>
</changeSet>
```

**Application Code**:
- Remove `Loan.getInstallmentAmountInMultiplesOf()` method
- Use `loan.getLoanProductRelatedDetail().getInstallmentAmountInMultiplesOf()` directly

---

## Step 4: Liquibase Synchronization

We need to fix Liquibase `databasechangelog` table to be compatible with migrations from develop branch.

**Execute BEFORE deploying from `develop`**:
```sql
INSERT INTO databasechangelog (id, author, filename, exectype, dateexecuted, orderexecuted, description, comments,
                               liquibase, contexts, deployment_id)
VALUES ('drop-index-1', 'fineract', 'db/changelog/tenant/module/loan/parts/1028_remove_suboptimal_indexes.xml',
        'EXECUTED', now(), (SELECT COALESCE(MAX(orderexecuted), 0) + 1 FROM databasechangelog),
        'dropIndex indexName=m_loan_transaction_transaction_type_enum_index, tableName=m_loan_transaction', '',
        '4.31.1', '(tenant_db AND !initial_switch)',
        (SELECT deployment_id FROM databasechangelog WHERE id = '1029-step-1-1')),
       ('drop-index-2', 'fineract', 'db/changelog/tenant/module/loan/parts/1028_remove_suboptimal_indexes.xml',
        'EXECUTED', now(), (SELECT COALESCE(MAX(orderexecuted), 0) + 2 FROM databasechangelog),
        'dropIndex indexName=m_loan_transaction_transaction_date_index, tableName=m_loan_transaction', '', '4.31.1',
        '(tenant_db AND !initial_switch)', (SELECT deployment_id FROM databasechangelog WHERE id = '1029-step-1-1')),
       ('drop-index-3', 'fineract', 'db/changelog/tenant/module/loan/parts/1028_remove_suboptimal_indexes.xml',
        'EXECUTED', now(), (SELECT COALESCE(MAX(orderexecuted), 0) + 3 FROM databasechangelog),
        'dropIndex indexName=m_loan_transaction_created_on_utc_index, tableName=m_loan_transaction', '', '4.31.1',
        '(tenant_db AND !initial_switch)', (SELECT deployment_id FROM databasechangelog WHERE id = '1029-step-1-1')),
       ('drop-index-4', 'fineract', 'db/changelog/tenant/module/loan/parts/1028_remove_suboptimal_indexes.xml',
        'EXECUTED', now(), (SELECT COALESCE(MAX(orderexecuted), 0) + 4 FROM databasechangelog),
        'dropIndex indexName=m_loan_transaction_is_reversed_index, tableName=m_loan_transaction', '', '4.31.1',
        '(tenant_db AND !initial_switch)', (SELECT deployment_id FROM databasechangelog WHERE id = '1029-step-1-1')),
       ('drop-index-5', 'fineract', 'db/changelog/tenant/module/loan/parts/1028_remove_suboptimal_indexes.xml',
        'EXECUTED', now(), (SELECT COALESCE(MAX(orderexecuted), 0) + 5 FROM databasechangelog),
        'dropIndex indexName=m_loan_transaction_submitted_on_date_index, tableName=m_loan_transaction', '', '4.31.1',
        '(tenant_db AND !initial_switch)', (SELECT deployment_id FROM databasechangelog WHERE id = '1029-step-1-1')),
       ('1029-1', 'fineract',
        'db/changelog/tenant/module/loan/parts/1029_add_installment_amount_in_multiples_of_to_loan.xml', 'EXECUTED',
        now(), (SELECT COALESCE(MAX(orderexecuted), 0) + 6 FROM databasechangelog), 'addColumn tableName=m_loan', '',
        '4.31.1', '(tenant_db AND !initial_switch)',
        (SELECT deployment_id FROM databasechangelog WHERE id = '1029-step-1-1')),
       ('1029-3-postgresql', 'fineract',
        'db/changelog/tenant/module/loan/parts/1029_add_installment_amount_in_multiples_of_to_loan.xml', 'EXECUTED',
        now(), (SELECT COALESCE(MAX(orderexecuted), 0) + 7 FROM databasechangelog), 'sql', '', '4.31.1',
        '(tenant_db AND !initial_switch)', (SELECT deployment_id FROM databasechangelog WHERE id = '1029-step-1-1')),
       ('1029-2', 'fineract',
        'db/changelog/tenant/module/loan/parts/1029_add_installment_amount_in_multiples_of_to_loan.xml', 'EXECUTED',
        now(), (SELECT COALESCE(MAX(orderexecuted), 0) + 8 FROM databasechangelog),
        'renameColumn newColumnName=installment_amount_in_multiples_of, oldColumnName=instalment_amount_in_multiples_of, tableName=m_product_loan',
        '', '4.31.1', '(tenant_db AND !initial_switch)',
        (SELECT deployment_id FROM databasechangelog WHERE id = '1029-step-1-1'));

DELETE FROM databasechangelog WHERE id IN ('1029-step-1-1', '1029-step-1-2', '1029-step-2', '1029-step-3');
```

**Key Features**:
- Insert migrations from `develop` branch: `1028_remove_suboptimal_indexes.xml` and `1029_add_installment_amount_in_multiples_of_to_loan.xml`
- Copy `deployment_id` from previous migration step to maintain consistency
- Increment `orderexecuted` according to current state
- Delete temporary migrations from Steps 1-3 as they are not part of `develop` branch migrations

---

## Conclusion

After completing all four steps, the `develop` branch can be safely deployed.
