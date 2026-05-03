# Payment Fraud Pipeline: Real-Time Detection with Flink SQL, ML_PREDICT, and AI_RUN_AGENT

This lab builds a multi-stage, real-time payment fraud detection pipeline entirely in Flink SQL on Confluent Cloud. No local producers or consumers are needed — all data generation and processing happens inside Confluent Cloud.

```
payments (faker) → Sanction Filter → Context Enricher → Risk Scorer (GPT-4o)
                         ↓                                       ↓ score > 0.8
                  payments_flagged ←──────────────────────────────┘
                         ↓
               Autonomous Resolver (AI_RUN_AGENT / GPT-4o)
                    ↓                    ↓
            payments_cleared    compliance_escalations
                    ↓
             Payment Gateway
```

**What gets provisioned:**
- 4 Kafka-backed Flink output tables (`payments_flagged`, `enriched_context`, `payments_cleared`, `compliance_escalations`)
- 1 faker-backed lookup table (`customer_history`)
- 1 OpenAI connection (`openai_conn`)
- 1 GPT-4o model (`fraud_scorer`)
- 1 Streaming Agent (`fraud_resolver_agent`)
- 8 continuous Flink SQL jobs (INSERT INTO statements)

---

## Prerequisites

- Confluent Cloud environment already deployed via `uv run deploy`
- OpenAI API key (get one at https://platform.openai.com/api-keys)
- Confluent Cloud Flink SQL workspace open (navigate to your environment → Flink → Open SQL Workspace)

---

## Deploy

This lab can be deployed automatically via Terraform or manually by running SQL statements in the Confluent Cloud UI.

### Option A: Automated Deployment (Recommended)

1. Add your OpenAI API key to `credentials.env`:
   ```bash
   echo 'TF_VAR_openai_api_key=sk-...' >> credentials.env
   ```

2. Deploy the lab using the automated tool:
   ```bash
   uv run deploy payment_fraud_pipeline
   ```

   > **Tip:** If you see a `401 Unauthorized` error (often caused by stale state from a previous run), use the `--clean` flag to wipe local state and start fresh:
   > `uv run deploy --clean payment_fraud_pipeline`

### Option B: Manual Deployment

Follow the steps below to run all SQL statements manually in the Confluent Cloud Flink UI.

---

## Step 0 — Verify the data source

The existing `payments` table (created by the fintech_lab deployment) is the source for this pipeline. Confirm it is generating data:

```sql
SELECT * FROM payments LIMIT 5;
```

Expected output:

| payment_id | customer_id | amount | transaction_type | country_code | transaction_ts |
|------------|-------------|--------|-----------------|--------------|----------------|
| uuid-...   | RAJN-0012   | 34.90  | PURCHASE        | US           | 2026-05-02 ... |
| uuid-...   | RAJN-0003   | 67.80  | CASH_ADVANCE    | GB           | 2026-05-02 ... |
| uuid-...   | RAJN-0041   | 8750.00| PURCHASE        | US           | 2026-05-02 ... |

---

## Step 1 — Create output tables (DDL)

Run each of these in the Flink SQL workspace. All use `IF NOT EXISTS` so they are safe to re-run.

### 1a. Flagged payments

```sql
CREATE TABLE IF NOT EXISTS `payments_flagged` (
  `payment_id`        VARCHAR(2147483647) NOT NULL,
  `customer_id`       VARCHAR(2147483647) NOT NULL,
  `customer_name`     VARCHAR(2147483647) NOT NULL,
  `merchant_name`     VARCHAR(2147483647) NOT NULL,
  `merchant_category` VARCHAR(2147483647) NOT NULL,
  `amount`            DOUBLE              NOT NULL,
  `payment_method`    VARCHAR(2147483647) NOT NULL,
  `card_type`         VARCHAR(2147483647) NOT NULL,
  `channel`           VARCHAR(2147483647) NOT NULL,
  `transaction_type`  VARCHAR(2147483647) NOT NULL,
  `country_code`      VARCHAR(2147483647) NOT NULL,
  `transaction_ts`    TIMESTAMP(3)        NOT NULL,
  `flag_reason`       VARCHAR(2147483647) NOT NULL
);
```

### 1b. Enriched context

```sql
CREATE TABLE IF NOT EXISTS `enriched_context` (
  `payment_id`        VARCHAR(2147483647) NOT NULL,
  `customer_id`       VARCHAR(2147483647) NOT NULL,
  `customer_name`     VARCHAR(2147483647) NOT NULL,
  `merchant_name`     VARCHAR(2147483647) NOT NULL,
  `merchant_category` VARCHAR(2147483647) NOT NULL,
  `amount`            DOUBLE              NOT NULL,
  `payment_method`    VARCHAR(2147483647) NOT NULL,
  `card_type`         VARCHAR(2147483647) NOT NULL,
  `channel`           VARCHAR(2147483647) NOT NULL,
  `transaction_type`  VARCHAR(2147483647) NOT NULL,
  `country_code`      VARCHAR(2147483647) NOT NULL,
  `transaction_ts`    TIMESTAMP(3)        NOT NULL,
  `total_tx_count`    BIGINT              NOT NULL,
  `total_spend`       DOUBLE              NOT NULL,
  `avg_amount`        DOUBLE              NOT NULL
);
```

### 1c. Cleared payments

```sql
CREATE TABLE IF NOT EXISTS `payments_cleared` (
  `payment_id`        VARCHAR(2147483647) NOT NULL,
  `customer_id`       VARCHAR(2147483647) NOT NULL,
  `amount`            DOUBLE              NOT NULL,
  `transaction_type`  VARCHAR(2147483647) NOT NULL,
  `transaction_ts`    TIMESTAMP(3)        NOT NULL,
  `risk_score`        DOUBLE              NOT NULL,
  `risk_tier`         VARCHAR(2147483647) NOT NULL,
  `reason`            VARCHAR(2147483647)
);
```

### 1d. Compliance escalations

```sql
CREATE TABLE IF NOT EXISTS `compliance_escalations` (
  `payment_id`     VARCHAR(2147483647) NOT NULL,
  `customer_id`    VARCHAR(2147483647) NOT NULL,
  `amount`         DOUBLE              NOT NULL,
  `flag_reason`    VARCHAR(2147483647) NOT NULL,
  `agent_response` VARCHAR(2147483647),
  `escalated_at`   TIMESTAMP(3)        NOT NULL
);
```

---

## Step 2 — Create the enriched payments source

Instead of a separate `customer_history` table joined at query time (which produces a retract stream that Kafka sinks can't accept), the history fields are generated inline in a single faker source table called `payments_with_history`. This keeps the output as a pure append stream.

```sql
CREATE TABLE IF NOT EXISTS `payments_with_history` (
  `payment_id`        VARCHAR(2147483647) NOT NULL,
  `customer_id`       VARCHAR(2147483647) NOT NULL,
  `customer_name`     VARCHAR(2147483647) NOT NULL,
  `merchant_name`     VARCHAR(2147483647) NOT NULL,
  `merchant_category` VARCHAR(2147483647) NOT NULL,
  `amount`            DOUBLE              NOT NULL,
  `payment_method`    VARCHAR(2147483647) NOT NULL,
  `card_type`         VARCHAR(2147483647) NOT NULL,
  `channel`           VARCHAR(2147483647) NOT NULL,
  `transaction_type`  VARCHAR(2147483647) NOT NULL,
  `country_code`      VARCHAR(2147483647) NOT NULL,
  `transaction_ts`    TIMESTAMP(3)        NOT NULL,
  `total_tx_count`    BIGINT              NOT NULL,
  `total_spend`       DOUBLE              NOT NULL,
  `avg_amount`        DOUBLE              NOT NULL,
  WATERMARK FOR `transaction_ts` AS `transaction_ts` - INTERVAL '5' SECOND
) WITH (
  'connector'       = 'faker',
  'rows-per-second' = '10',
  'fields.payment_id.expression'        = '#{Internet.uuid}',
  'fields.customer_id.expression'       = '#{Options.option ''RAJN-0001'',''RAJN-0002'',''RAJN-0003'',''RAJN-0004'',''RAJN-0005'',''RAJN-0006'',''RAJN-0007'',''RAJN-0008'',''RAJN-0009'',''RAJN-0010'',''RAJN-0011'',''RAJN-0012'',''RAJN-0013'',''RAJN-0014'',''RAJN-0015'',''RAJN-0016'',''RAJN-0017'',''RAJN-0018'',''RAJN-0019'',''RAJN-0020'',''RAJN-0021'',''RAJN-0022'',''RAJN-0023'',''RAJN-0024'',''RAJN-0025'',''RAJN-0026'',''RAJN-0027'',''RAJN-0028'',''RAJN-0029'',''RAJN-0030'',''RAJN-0031'',''RAJN-0032'',''RAJN-0033'',''RAJN-0034'',''RAJN-0035'',''RAJN-0036'',''RAJN-0037'',''RAJN-0038'',''RAJN-0039'',''RAJN-0040'',''RAJN-0041'',''RAJN-0042'',''RAJN-0043'',''RAJN-0044'',''RAJN-0045'',''RAJN-0046'',''RAJN-0047'',''RAJN-0048'',''RAJN-0049'',''RAJN-0050''}',
  'fields.customer_name.expression'     = '#{Options.option ''Rahul'',''Priya'',''Amit'',''Sneha'',''Rohan'',''Anjali'',''Vikram'',''Pooja'',''Sanjay'',''Neha''}',
  'fields.merchant_name.expression'     = '#{Company.name}',
  'fields.merchant_category.expression' = '#{Options.option ''GROCERY'',''GROCERY'',''GROCERY'',''RESTAURANT'',''RESTAURANT'',''ELECTRONICS'',''TRAVEL'',''OTHER''}',
  'fields.amount.expression'            = '#{Options.option ''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''8750.00''}',
  'fields.payment_method.expression'    = '#{Options.option ''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''DEBIT_CARD'',''DEBIT_CARD'',''WIRE_TRANSFER''}',
  'fields.card_type.expression'         = '#{Options.option ''VISA'',''VISA'',''MASTERCARD'',''MASTERCARD'',''AMEX'',''DISCOVER''}',
  'fields.channel.expression'           = '#{Options.option ''IN_STORE'',''IN_STORE'',''ONLINE'',''MOBILE_APP'',''ATM''}',
  'fields.transaction_type.expression'  = '#{Options.option ''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''REFUND'',''CASH_ADVANCE''}',
  'fields.country_code.expression'      = '#{Options.option ''US'',''US'',''US'',''GB''}',
  'fields.transaction_ts.expression'    = '#{date.past ''5'',''SECONDS''}',
  'fields.total_tx_count.expression'    = '#{number.numberBetween ''10'',''500''}',
  'fields.total_spend.expression'       = '#{number.randomDouble ''2'',''500'',''50000''}',
  'fields.avg_amount.expression'        = '#{number.randomDouble ''2'',''20'',''200''}'
);
```

Verify:

```sql
SELECT customer_id, amount, total_tx_count, total_spend, avg_amount
FROM payments_with_history LIMIT 5;
```

> **Why not a separate `customer_history` table with a JOIN?**
> A streaming `LEFT JOIN` between two unbounded tables produces a retract/update changelog — Flink must re-emit corrected rows whenever new history arrives. Kafka-backed append-only sinks reject those retract messages with the error you saw. Inlining the history fields into the faker source avoids the join entirely and keeps the stream append-only.

---

## Step 3 — Register the OpenAI connection and model

### 3a. Create the OpenAI connection

Replace `<YOUR_OPENAI_API_KEY>` with your actual key. The key value will show as `<REDACTED>` after creation — that is correct.

```sql
CREATE CONNECTION IF NOT EXISTS `openai_conn`
WITH (
  'type'     = 'openai',
  'endpoint' = 'https://api.openai.com/v1/chat/completions',
  'api-key'  = '<YOUR_OPENAI_API_KEY>'
);
```

Verify:

```sql
DESCRIBE CONNECTION `openai_conn`;
```

### 3b. Create the fraud scorer model

```sql
CREATE MODEL IF NOT EXISTS `fraud_scorer`
INPUT  (prompt STRING)
OUTPUT (response STRING)
COMMENT 'GPT-4o fraud risk scorer for payment fraud pipeline'
WITH (
  'provider'             = 'openai',
  'task'                 = 'text_generation',
  'openai.connection'    = 'openai_conn',
  'openai.model_version' = 'gpt-4o',
  'openai.system_prompt' = 'You are a financial fraud analyst. Analyze the payment transaction and respond with ONLY a valid JSON object. No markdown, no explanation, no extra text. Format: {"risk_score": <float 0.0-1.0>, "risk_tier": "<LOW|MEDIUM|HIGH>", "reason": "<10 words max>"}'
);
```

Verify:

```sql
SHOW MODELS;
```

### 3c. Test the model (optional — run before starting jobs)

```sql
SELECT
  p.customer_id,
  p.amount,
  p.transaction_type,
  r.response                                                          AS raw_response,
  CAST(JSON_VALUE(r.response, '$.risk_score') AS DOUBLE)             AS risk_score,
  JSON_VALUE(r.response, '$.risk_tier')                              AS risk_tier,
  JSON_VALUE(r.response, '$.reason')                                 AS reason
FROM payments AS p,
LATERAL TABLE(
  ML_PREDICT(
    'fraud_scorer',
    CONCAT(
      'Transaction: customer_id=', p.customer_id,
      ', amount=', CAST(p.amount AS STRING),
      ', type=', p.transaction_type,
      ', country=', p.country_code
    )
  )
) AS r
LIMIT 3;
```

---

## Step 4 — Create the Streaming Agent

```sql
CREATE AGENT IF NOT EXISTS `fraud_resolver_agent`
USING MODEL `fraud_scorer`
USING PROMPT 'You are an autonomous fraud resolution agent. Review the flagged payment details and decide: respond with RESOLVED if the payment appears legitimate based on context, or ESCALATED if it requires human compliance review. Always include your decision word (RESOLVED or ESCALATED) at the start of your response, followed by a brief justification of 15 words or fewer.'
WITH (
  'max_iterations' = '3'
);
```

---

## Step 5 — Start the pipeline (INSERT INTO jobs)

Run these in order. Each is a continuous Flink job — it will keep running until you stop it.

### Stage 1: Sanction Filter

**Job A — Flag sanctioned payments** (5 customer IDs, ~10% of traffic):

```sql
INSERT INTO `payments_flagged`
SELECT
  payment_id, customer_id, customer_name, merchant_name, merchant_category,
  amount, payment_method, card_type, channel, transaction_type, country_code,
  transaction_ts,
  'SANCTION_MATCH' AS flag_reason
FROM `payments_with_history`
WHERE customer_id IN ('RAJN-0003', 'RAJN-0007', 'RAJN-0015', 'RAJN-0023', 'RAJN-0041');
```

**Job B — Pass non-sanctioned payments directly to enriched_context:**

```sql
INSERT INTO `enriched_context`
SELECT
  payment_id, customer_id, customer_name, merchant_name, merchant_category,
  amount, payment_method, card_type, channel, transaction_type, country_code,
  transaction_ts,
  total_tx_count,
  total_spend,
  avg_amount
FROM `payments_with_history`
WHERE customer_id NOT IN ('RAJN-0003', 'RAJN-0007', 'RAJN-0015', 'RAJN-0023', 'RAJN-0041');
```

Verify after ~10 seconds:

```sql
SELECT * FROM payments_flagged LIMIT 5;
```

Expected output:

| payment_id | customer_id | amount | flag_reason    |
|------------|-------------|--------|----------------|
| uuid-...   | RAJN-0003   | 45.20  | SANCTION_MATCH |
| uuid-...   | RAJN-0041   | 8750.00| SANCTION_MATCH |
| uuid-...   | RAJN-0007   | 23.75  | SANCTION_MATCH |

---

### Stage 2: Context Enricher (built into Stage 1 Job B)

The enrichment step is now embedded in the sanction-pass job above — non-sanctioned payments flow directly into `enriched_context` with history fields already attached from `payments_with_history`. No separate job is needed.

Verify:

```sql
SELECT customer_id, amount, total_tx_count, total_spend, avg_amount
FROM enriched_context
LIMIT 5;
```

Expected output:

| customer_id | amount | total_tx_count | total_spend | avg_amount |
|-------------|--------|----------------|-------------|------------|
| RAJN-0012   | 34.90  | 247            | 18432.50    | 74.60      |
| RAJN-0028   | 67.80  | 89             | 5210.00     | 58.54      |
| RAJN-0001   | 110.75 | 412            | 31890.00    | 77.40      |

---

### Stage 3: Risk Scorer

Calls GPT-4o via `ML_PREDICT` for every enriched payment. Payments with `risk_score > 0.8` go to `payments_flagged`; all others go to `payments_cleared`.

> **Note:** GPT-4o calls add latency. Expect a short delay before records appear in output tables. The `ML_PREDICT` function is asynchronous by default (`async_enabled=true`) with up to 10 parallel requests.

**Job A — High-risk → payments_flagged:**

```sql
INSERT INTO `payments_flagged`
SELECT
  e.payment_id, e.customer_id, e.customer_name, e.merchant_name, e.merchant_category,
  e.amount, e.payment_method, e.card_type, e.channel, e.transaction_type,
  e.country_code, e.transaction_ts,
  'HIGH_RISK_SCORE' AS flag_reason
FROM `enriched_context` AS e,
LATERAL TABLE(
  ML_PREDICT(
    'fraud_scorer',
    CONCAT(
      'Transaction: customer_id=', e.customer_id,
      ', amount=', CAST(e.amount AS STRING),
      ', type=', e.transaction_type,
      ', merchant_category=', e.merchant_category,
      ', country=', e.country_code,
      ', tx_count=', CAST(e.total_tx_count AS STRING),
      ', total_spend=', CAST(e.total_spend AS STRING),
      ', avg_amount=', CAST(e.avg_amount AS STRING)
    )
  )
) AS r
WHERE COALESCE(CAST(JSON_VALUE(r.response, '$.risk_score') AS DOUBLE), -1.0) > 0.8;
```

**Job B — Low-risk → payments_cleared:**

```sql
INSERT INTO `payments_cleared`
SELECT
  e.payment_id,
  e.customer_id,
  e.amount,
  e.transaction_type,
  e.transaction_ts,
  COALESCE(CAST(JSON_VALUE(r.response, '$.risk_score') AS DOUBLE), -1.0) AS risk_score,
  COALESCE(JSON_VALUE(r.response, '$.risk_tier'), 'PARSE_ERROR')          AS risk_tier,
  JSON_VALUE(r.response, '$.reason')                                       AS reason
FROM `enriched_context` AS e,
LATERAL TABLE(
  ML_PREDICT(
    'fraud_scorer',
    CONCAT(
      'Transaction: customer_id=', e.customer_id,
      ', amount=', CAST(e.amount AS STRING),
      ', type=', e.transaction_type,
      ', merchant_category=', e.merchant_category,
      ', country=', e.country_code,
      ', tx_count=', CAST(e.total_tx_count AS STRING),
      ', total_spend=', CAST(e.total_spend AS STRING),
      ', avg_amount=', CAST(e.avg_amount AS STRING)
    )
  )
) AS r
WHERE COALESCE(CAST(JSON_VALUE(r.response, '$.risk_score') AS DOUBLE), -1.0) <= 0.8;
```

Verify:

```sql
SELECT customer_id, amount, risk_score, risk_tier, reason
FROM payments_cleared
LIMIT 5;
```

Expected output:

| customer_id | amount | risk_score | risk_tier | reason                              |
|-------------|--------|------------|-----------|-------------------------------------|
| RAJN-0012   | 34.90  | 0.12       | LOW       | Normal grocery purchase, low amount |
| RAJN-0028   | 67.80  | 0.35       | LOW       | Routine restaurant transaction      |
| RAJN-0001   | 110.75 | 0.61       | MEDIUM    | Slightly above average spend        |

---

### Stage 4: Autonomous Resolver

Reads every record from `payments_flagged` and calls the `fraud_resolver_agent` via `AI_RUN_AGENT`. The agent reasons over the payment context and returns either `RESOLVED` or `ESCALATED`.

> **Note:** `AI_RUN_AGENT` is a Streaming Agent feature. Ensure your Confluent Cloud environment has Streaming Agents enabled. See [Create and Run Streaming Agents](https://docs.confluent.io/cloud/current/ai/streaming-agents/create-and-run-streaming-agents.html).

**Job A — Resolved → payments_cleared:**

```sql
INSERT INTO `payments_cleared`
SELECT
  f.payment_id,
  f.customer_id,
  f.amount,
  f.transaction_type,
  f.transaction_ts,
  0.0              AS risk_score,
  'AGENT_RESOLVED' AS risk_tier,
  a.response       AS reason
FROM `payments_flagged` AS f,
LATERAL TABLE(
  AI_RUN_AGENT(
    'fraud_resolver_agent',
    CONCAT(
      'Flagged payment: payment_id=', f.payment_id,
      ', customer_id=', f.customer_id,
      ', amount=', CAST(f.amount AS STRING),
      ', type=', f.transaction_type,
      ', flag_reason=', f.flag_reason,
      ', country=', f.country_code
    ),
    f.payment_id
  )
) AS a
WHERE a.response LIKE '%RESOLVED%';
```

**Job B — Escalated → compliance_escalations:**

```sql
INSERT INTO `compliance_escalations`
SELECT
  f.payment_id,
  f.customer_id,
  f.amount,
  f.flag_reason,
  COALESCE(a.response, 'AGENT_ERROR') AS agent_response,
  f.transaction_ts                     AS escalated_at
FROM `payments_flagged` AS f,
LATERAL TABLE(
  AI_RUN_AGENT(
    'fraud_resolver_agent',
    CONCAT(
      'Flagged payment: payment_id=', f.payment_id,
      ', customer_id=', f.customer_id,
      ', amount=', CAST(f.amount AS STRING),
      ', type=', f.transaction_type,
      ', flag_reason=', f.flag_reason,
      ', country=', f.country_code
    ),
    f.payment_id
  )
) AS a
WHERE a.response NOT LIKE '%RESOLVED%' OR a.response IS NULL;
```

Verify:

```sql
SELECT payment_id, customer_id, amount, flag_reason, agent_response
FROM compliance_escalations
LIMIT 5;
```

Expected output:

| payment_id | customer_id | amount  | flag_reason      | agent_response                                    |
|------------|-------------|---------|------------------|---------------------------------------------------|
| uuid-...   | RAJN-0041   | 8750.00 | HIGH_RISK_SCORE  | ESCALATED — unusually large amount, no history    |
| uuid-...   | RAJN-0003   | 45.20   | SANCTION_MATCH   | ESCALATED — customer on sanctions list            |

---

## Observability — Diagnostic Queries

Run these at any time in the Flink SQL workspace to monitor pipeline health.

### 1. Breakdown of flagged payments by reason

```sql
SELECT flag_reason, COUNT(*) AS cnt
FROM payments_flagged
GROUP BY flag_reason;
```

Expected: two rows — `SANCTION_MATCH` (~10% of traffic) and `HIGH_RISK_SCORE` (GPT-4o scored > 0.8).

### 2. Breakdown of cleared payments by risk tier

```sql
SELECT risk_tier, COUNT(*) AS cnt
FROM payments_cleared
GROUP BY risk_tier;
```

Expected: `LOW`, `MEDIUM`, and `AGENT_RESOLVED` tiers.

### 3. GPT-4o parse error rate

```sql
SELECT
  COUNT(*) FILTER (WHERE risk_score = -1.0)  AS parse_errors,
  COUNT(*) FILTER (WHERE risk_tier = 'HIGH') AS high_risk_count,
  COUNT(*)                                    AS total_scored
FROM payments_cleared;
```

A `parse_errors` count above 5% indicates GPT-4o is returning malformed JSON — check the `raw_response` column if you added it to the table.

### 4. Enriched context spot check

```sql
SELECT customer_id, amount, total_tx_count, total_spend, avg_amount
FROM enriched_context
LIMIT 10;
```

### 5. Compliance escalations

```sql
SELECT *
FROM compliance_escalations
ORDER BY escalated_at DESC
LIMIT 10;
```

---

## Architecture Summary

| Component | Confluent Feature | Notes |
|-----------|------------------|-------|
| Data source | Flink faker connector | Reuses existing `payments` table |
| Sanction filter | Flink SQL `INSERT INTO` + `WHERE IN` | 5 hardcoded sanctioned IDs |
| Customer history | Flink faker connector | Inline in `payments_with_history` source |
| Context enrichment | Flink faker connector | History fields generated inline, no JOIN needed |
| Risk scoring | `ML_PREDICT` + OpenAI GPT-4o | JSON response parsing with sentinel |
| Autonomous resolution | `AI_RUN_AGENT` + Streaming Agent | RESOLVED/ESCALATED routing |
| VertexAI/SageMaker (diagram) | Replaced by OpenAI `ML_PREDICT` | Same `ML_PREDICT` call pattern |
| WatsonX Orchestrate (diagram) | Replaced by Confluent Streaming Agent | `CREATE AGENT` + `AI_RUN_AGENT` |

---

## Cleanup

```bash
# Destroy only the payment fraud pipeline
uv run destroy payment_fraud_pipeline

# Or destroy everything (core + fintech_lab + payment_fraud_pipeline)
uv run destroy
```

> **Note:** The `payments` source table and core infrastructure (environment, cluster, compute pool) are shared with the fintech_lab. Destroying `payment_fraud_pipeline` only removes the pipeline-specific Flink statements. The core resources are managed by `terraform/core`.

---

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **← Fintech Lab**: [FintechLab-Walkthrough.md](./FintechLab-Walkthrough.md)
- **📖 Confluent Docs**: [AI Model Inference](https://docs.confluent.io/cloud/current/ai/ai-model-inference.html) | [Streaming Agents](https://docs.confluent.io/cloud/current/ai/streaming-agents/create-and-run-streaming-agents.html) | [ML_PREDICT](https://docs.confluent.io/cloud/current/flink/reference/functions/model-inference-functions.html)
