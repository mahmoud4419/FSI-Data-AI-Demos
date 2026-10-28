-- ================================================================
-- RETAIL BANKING DEMO — Redshift SQL Scripts
-- ================================================================
-- Architecture: Kinesis → Redshift Streaming Ingestion → RedshiftML
-- Author: Mahmoud Elserafi, Specialist SA — Data & Analytics, AWS
-- ================================================================


-- ============================================================
-- STEP 1: Create External Schema for Kinesis Streaming
-- ============================================================
-- Replace <iam-role-arn> with your Redshift IAM role ARN
-- Replace <kinesis-stream-arn> with your Kinesis stream ARN

CREATE EXTERNAL SCHEMA IF NOT EXISTS banking_kinesis
FROM KINESIS
IAM_ROLE '<iam-role-arn>';


-- ============================================================
-- STEP 2: Create Streaming Materialized View
-- ============================================================
-- RULES:
--   1. kinesis_data is varbyte → must use from_varbyte()
--   2. All columns MUST be VARCHAR (no type casts in streaming MVs)
--   3. Cast types in downstream consuming queries

CREATE MATERIALIZED VIEW banking.mv_live_transactions AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'txn_id')              AS txn_id,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'txn_timestamp')       AS txn_timestamp,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'customer_name')       AS customer_name,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'txn_type')            AS txn_type,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'amount')              AS amount,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'currency')            AS currency,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'channel')             AS channel,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'location')            AS location,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'account_id')          AS account_id,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'account_age_months')  AS account_age_months,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'avg_balance')         AS avg_balance,
    json_extract_path_text(from_varbyte(kinesis_data, 'utf-8'), 'credit_score')        AS credit_score
FROM banking_kinesis."banking-transactions-stream";

-- Refresh (run periodically or auto-refresh)
REFRESH MATERIALIZED VIEW banking.mv_live_transactions;


-- ============================================================
-- STEP 3: Create Training Data Table
-- ============================================================
-- Upload banking_training_data.csv to S3 first, then COPY

CREATE TABLE banking.fraud_training_data (
    txn_id                  INTEGER,
    txn_timestamp           TIMESTAMP,
    customer_name           VARCHAR(100),
    txn_type                VARCHAR(50),
    amount                  DECIMAL(12,2),
    currency                VARCHAR(10),
    channel                 VARCHAR(50),
    location                VARCHAR(100),
    account_id              BIGINT,
    account_age_months      INTEGER,
    avg_balance             DECIMAL(12,2),
    credit_score            INTEGER,
    amt_to_balance_ratio    DECIMAL(10,4),
    is_international        INTEGER,
    is_high_risk_channel    INTEGER,
    hour_of_day             INTEGER,
    is_weekend              INTEGER,
    is_night                INTEGER,
    is_fraud                INTEGER        -- TARGET COLUMN (0 = legitimate, 1 = fraud)
);

-- Load from S3
COPY banking.fraud_training_data
FROM 's3://<your-bucket>/banking-demo/banking_training_data.csv'
IAM_ROLE '<iam-role-arn>'
CSV IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto';

-- Verify data loaded
SELECT COUNT(*) AS total_rows,
       SUM(is_fraud) AS fraud_count,
       ROUND(100.0 * SUM(is_fraud) / COUNT(*), 1) AS fraud_pct
FROM banking.fraud_training_data;


-- ============================================================
-- STEP 4: Create RedshiftML Fraud Detection Model
-- ============================================================
-- RULES:
--   1. Model name must be SHORT (auto-generated S3 path < 128 chars)
--   2. Requires sagemaker:* wildcard IAM permissions
--   3. Requires ≥500 rows of training data
--   4. Do NOT use S3_OUTPUT_PATH (not a valid setting)

CREATE MODEL banking.fraud_model
FROM (
    SELECT
        amount,
        account_age_months,
        avg_balance,
        credit_score,
        amt_to_balance_ratio,
        is_international,
        is_high_risk_channel,
        hour_of_day,
        is_weekend,
        is_night,
        is_fraud
    FROM banking.fraud_training_data
)
TARGET is_fraud
FUNCTION fn_detect_fraud
IAM_ROLE '<iam-role-arn>'
SETTINGS (
    S3_BUCKET '<your-bucket>',
    MAX_RUNTIME 3600
);

-- Check model training status (takes ~15-30 minutes)
SHOW MODEL banking.fraud_model;


-- ============================================================
-- STEP 5: Grant Permissions (use MODEL, not FUNCTION)
-- ============================================================
-- RULE: GRANT EXECUTE ON FUNCTION fails for ML inference functions
-- Must use: GRANT EXECUTE ON MODEL

GRANT EXECUTE ON MODEL banking.fraud_model TO PUBLIC;


-- ============================================================
-- STEP 6: Test Predictions on Training Data
-- ============================================================
-- RULE: Always use LIMIT when calling ML functions
-- (each row invokes SageMaker endpoint — too many rows → timeout)

SELECT
    txn_id,
    customer_name,
    amount,
    location,
    is_fraud AS actual_fraud,
    banking.fn_detect_fraud(
        amount, account_age_months, avg_balance, credit_score,
        amt_to_balance_ratio, is_international, is_high_risk_channel,
        hour_of_day, is_weekend, is_night
    ) AS predicted_fraud
FROM banking.fraud_training_data
LIMIT 10;


-- ============================================================
-- STEP 7: Real-Time Fraud Scoring on Live Stream
-- ============================================================
-- RULE: Use CTE pattern + LIMIT to avoid timeout
-- RULE: Cast VARCHAR columns from streaming MV to proper types

WITH live_scored AS (
    SELECT
        txn_id,
        customer_name,
        txn_type,
        CAST(amount AS DECIMAL(12,2)) AS amount,
        channel,
        location,
        CAST(account_age_months AS INTEGER) AS acct_age,
        CAST(avg_balance AS DECIMAL(12,2)) AS avg_bal,
        CAST(credit_score AS INTEGER) AS credit_sc,
        -- Derived features
        CAST(amount AS DECIMAL(12,2)) / NULLIF(CAST(avg_balance AS DECIMAL(12,2)), 0) AS amt_ratio,
        CASE WHEN location IN ('Lagos','London','New York','Mumbai','São Paulo') THEN 1 ELSE 0 END AS is_intl,
        CASE WHEN channel IN ('online_banking','atm') THEN 1 ELSE 0 END AS is_high_risk_ch,
        EXTRACT(HOUR FROM CAST(txn_timestamp AS TIMESTAMP)) AS hr,
        CASE WHEN EXTRACT(DOW FROM CAST(txn_timestamp AS TIMESTAMP)) IN (0,6) THEN 1 ELSE 0 END AS is_wknd,
        CASE WHEN EXTRACT(HOUR FROM CAST(txn_timestamp AS TIMESTAMP)) < 6
              OR EXTRACT(HOUR FROM CAST(txn_timestamp AS TIMESTAMP)) > 22 THEN 1 ELSE 0 END AS is_nght
    FROM banking.mv_live_transactions
    ORDER BY approximate_arrival_timestamp DESC
    LIMIT 5
)
SELECT
    txn_id,
    customer_name,
    txn_type,
    amount,
    location,
    banking.fn_detect_fraud(
        amount, acct_age, avg_bal, credit_sc,
        amt_ratio, is_intl, is_high_risk_ch,
        hr, is_wknd, is_nght
    ) AS fraud_prediction,
    CASE
        WHEN banking.fn_detect_fraud(
            amount, acct_age, avg_bal, credit_sc,
            amt_ratio, is_intl, is_high_risk_ch,
            hr, is_wknd, is_nght
        ) = 1 THEN '🚨 FLAGGED'
        ELSE '✅ CLEAR'
    END AS status
FROM live_scored;


-- ============================================================
-- STEP 8: Create Credit Risk Scoring Model (Bonus)
-- ============================================================
-- Uses the same training data but targets credit_score prediction

CREATE MODEL banking.credit_risk_model
FROM (
    SELECT
        amount,
        account_age_months,
        avg_balance,
        is_international,
        is_high_risk_channel,
        credit_score    -- TARGET: predict credit score
    FROM banking.fraud_training_data
)
TARGET credit_score
FUNCTION fn_credit_risk
IAM_ROLE '<iam-role-arn>'
SETTINGS (
    S3_BUCKET '<your-bucket>',
    MAX_RUNTIME 3600
);


-- ============================================================
-- STEP 9: Useful Monitoring Queries
-- ============================================================

-- Count live transactions
SELECT COUNT(*) AS live_txn_count
FROM banking.mv_live_transactions;

-- Transaction volume by channel (last 1 hour)
SELECT channel, COUNT(*) AS txn_count, SUM(CAST(amount AS DECIMAL(12,2))) AS total_amount
FROM banking.mv_live_transactions
WHERE approximate_arrival_timestamp > GETDATE() - INTERVAL '1 hour'
GROUP BY channel
ORDER BY txn_count DESC;

-- Fraud summary
SELECT
    location,
    COUNT(*) AS total_txns,
    SUM(is_fraud) AS fraud_count,
    ROUND(100.0 * SUM(is_fraud) / COUNT(*), 1) AS fraud_rate_pct
FROM banking.fraud_training_data
GROUP BY location
ORDER BY fraud_rate_pct DESC;
