data "terraform_remote_state" "core" {
  backend = "local"
  config  = { path = "../core/terraform.tfstate" }
}

locals {
  environment_id     = data.terraform_remote_state.core.outputs.confluent_environment_id
  compute_pool_id    = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  organization_id    = data.terraform_remote_state.core.outputs.confluent_organization_id
  service_account_id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  flink_rest_endpoint = data.terraform_remote_state.core.outputs.confluent_flink_rest_endpoint
  flink_api_key      = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
  flink_api_secret   = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  random_id          = data.terraform_remote_state.core.outputs.random_id

  flink_properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# DDL: Staging / output tables
# All use CREATE TABLE IF NOT EXISTS for idempotent deployments.
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "create_payments_flagged" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-payments-flagged-${local.random_id}"

  statement = <<-SQL
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
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}

resource "confluent_flink_statement" "create_enriched_context" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-enriched-context-${local.random_id}"

  statement = <<-SQL
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
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}

resource "confluent_flink_statement" "create_payments_cleared" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-payments-cleared-${local.random_id}"

  statement = <<-SQL
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
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}

resource "confluent_flink_statement" "create_compliance_escalations" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-compliance-escalations-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS `compliance_escalations` (
      `payment_id`     VARCHAR(2147483647) NOT NULL,
      `customer_id`    VARCHAR(2147483647) NOT NULL,
      `amount`         DOUBLE              NOT NULL,
      `flag_reason`    VARCHAR(2147483647) NOT NULL,
      `agent_response` VARCHAR(2147483647),
      `escalated_at`   TIMESTAMP(3)        NOT NULL
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# DDL: payments_with_history — faker source that generates enriched payment rows
#      WITH inline history columns already attached.
#
# Why: A streaming LEFT JOIN between two unbounded faker tables produces a
# retract/update changelog that Kafka-backed append-only sinks cannot accept.
# The fix is to generate history fields (total_tx_count, total_spend,
# avg_amount) directly in the faker source expression, eliminating the join
# entirely and keeping the output as a pure append stream.
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_payments_with_history" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-payments-with-history-${local.random_id}"

  statement = <<-SQL
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
      'rows-per-second' = '1',
      'fields.payment_id.expression'        = '#{Internet.uuid}',
      'fields.customer_id.expression'       = '#{Options.option ''RAJN-0001'',''RAJN-0002'',''RAJN-0003'',''RAJN-0004'',''RAJN-0005'',''RAJN-0006'',''RAJN-0007'',''RAJN-0008'',''RAJN-0009'',''RAJN-0010'',''RAJN-0011'',''RAJN-0012'',''RAJN-0013'',''RAJN-0014'',''RAJN-0015'',''RAJN-0016'',''RAJN-0017'',''RAJN-0018'',''RAJN-0019'',''RAJN-0020'',''RAJN-0021'',''RAJN-0022'',''RAJN-0023'',''RAJN-0024'',''RAJN-0025'',''RAJN-0026'',''RAJN-0027'',''RAJN-0028'',''RAJN-0029'',''RAJN-0030'',''RAJN-0031'',''RAJN-0032'',''RAJN-0033'',''RAJN-0034'',''RAJN-0035'',''RAJN-0036'',''RAJN-0037'',''RAJN-0038'',''RAJN-0039'',''RAJN-0040'',''RAJN-0041'',''RAJN-0042'',''RAJN-0043'',''RAJN-0044'',''RAJN-0045'',''RAJN-0046'',''RAJN-0047'',''RAJN-0048'',''RAJN-0049'',''RAJN-0050''}',
      'fields.customer_name.expression'     = '#{Options.option ''Rahul'',''Priya'',''Amit'',''Sneha'',''Rohan'',''Anjali'',''Vikram'',''Pooja'',''Sanjay'',''Neha''}',
      'fields.merchant_name.expression'     = '#{Company.name}',
      'fields.merchant_category.expression' = '#{Options.option ''GROCERY'',''GROCERY'',''GROCERY'',''RESTAURANT'',''RESTAURANT'',''ELECTRONICS'',''TRAVEL'',''OTHER''}',
      'fields.amount.expression'            = '#{Options.option ''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''12.50'',''23.75'',''34.90'',''45.20'',''56.40'',''67.80'',''78.10'',''89.99'',''95.50'',''110.75'',''8750.00''}',
      'fields.payment_method.expression'    = '#{Options.option ''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''CREDIT_CARD'',''DEBIT_CARD'',''DEBIT_CARD'',''DEBIT_CARD'',''WIRE_TRANSFER''}',
      'fields.card_type.expression'         = '#{Options.option ''VISA'',''VISA'',''VISA'',''MASTERCARD'',''MASTERCARD'',''AMEX'',''DISCOVER''}',
      'fields.channel.expression'           = '#{Options.option ''IN_STORE'',''IN_STORE'',''IN_STORE'',''IN_STORE'',''ONLINE'',''ONLINE'',''MOBILE_APP'',''ATM''}',
      'fields.transaction_type.expression'  = '#{Options.option ''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''PURCHASE'',''REFUND'',''CASH_ADVANCE''}',
      'fields.country_code.expression'      = '#{Options.option ''US'',''US'',''US'',''GB''}',
      'fields.transaction_ts.expression'    = '#{date.past ''5'',''SECONDS''}',
      'fields.total_tx_count.expression'    = '#{number.numberBetween ''10'',''500''}',
      'fields.total_spend.expression'       = '#{number.randomDouble ''2'',''500'',''50000''}',
      'fields.avg_amount.expression'        = '#{number.randomDouble ''2'',''20'',''200''}'
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# DDL: OpenAI connection
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_openai_conn" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-openai-conn-${local.random_id}"

  statement = <<-SQL
    CREATE CONNECTION IF NOT EXISTS `openai_conn`
    WITH (
      'type'     = 'openai',
      'endpoint' = 'https://api.openai.com/v1/chat/completions',
      'api-key'  = '${var.openai_api_key}'
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# DDL: fraud_scorer model (GPT-4o)
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_fraud_scorer" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-fraud-scorer-${local.random_id}"

  statement = <<-SQL
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
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [confluent_flink_statement.create_openai_conn]
}

# ─────────────────────────────────────────────────────────────────────────────
# DDL: fraud_resolver_agent (Streaming Agent)
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_fraud_resolver_agent" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-fraud-resolver-agent-${local.random_id}"

  statement = <<-SQL
    CREATE AGENT IF NOT EXISTS `fraud_resolver_agent`
    USING MODEL `fraud_scorer`
    USING PROMPT 'You are an autonomous fraud resolution agent. Review the flagged payment details and decide: respond with RESOLVED if the payment appears legitimate based on context, or ESCALATED if it requires human compliance review. Always include your decision word (RESOLVED or ESCALATED) at the start of your response, followed by a brief justification of 15 words or fewer.'
    WITH (
      'max_iterations' = '3'
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [confluent_flink_statement.create_fraud_scorer]
}

# ─────────────────────────────────────────────────────────────────────────────
# DML: Stage 1 — Sanction Filter
#
# Reads from payments_with_history (the enriched faker source).
# Sanction list: RAJN-0003, RAJN-0007, RAJN-0015, RAJN-0023, RAJN-0041
# (~10% of the 50-customer pool for demo visibility)
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "insert_sanction_flagged" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-insert-sanction-flagged-${local.random_id}"

  statement = <<-SQL
    INSERT INTO `payments_flagged`
    SELECT
      payment_id, customer_id, customer_name, merchant_name, merchant_category,
      amount, payment_method, card_type, channel, transaction_type, country_code,
      transaction_ts,
      'SANCTION_MATCH' AS flag_reason
    FROM `payments_with_history`
    WHERE customer_id IN ('RAJN-0003', 'RAJN-0007', 'RAJN-0015', 'RAJN-0023', 'RAJN-0041');
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_flink_statement.create_payments_flagged,
    confluent_flink_statement.create_enriched_context,
    confluent_flink_statement.create_payments_cleared,
    confluent_flink_statement.create_compliance_escalations,
    confluent_flink_statement.create_payments_with_history,
  ]
}

resource "confluent_flink_statement" "insert_sanction_passed" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-insert-sanction-passed-${local.random_id}"

  # Non-sanctioned payments flow directly into enriched_context — the history
  # fields are already present on payments_with_history, so no join is needed.
  statement = <<-SQL
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
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_flink_statement.create_enriched_context,
    confluent_flink_statement.create_payments_with_history,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# DDL: enriched_scored — intermediate table for ML_PREDICT results
#
# ML_PREDICT (async correlate) cannot have WHERE clauses or projections of
# the response columns in the same statement. The fix is a two-step pattern:
#   Step A: ML_PREDICT writes ALL rows + raw response to enriched_scored
#   Step B: Plain SQL reads enriched_scored and routes to flagged/cleared
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_enriched_scored" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-create-enriched-scored-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS `enriched_scored` (
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
      `raw_response`      VARCHAR(2147483647)
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# DML: Stage 3a — ML_PREDICT (no WHERE, no response projection)
#
# Async correlate only supports passing through source columns + the raw
# model output. All filtering and JSON parsing happens in Stage 3b.
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "insert_ml_predict" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-insert-ml-predict-${local.random_id}"

  statement = <<-SQL
    INSERT INTO `enriched_scored`
    SELECT
      e.payment_id,
      e.customer_id,
      e.customer_name,
      e.merchant_name,
      e.merchant_category,
      e.amount,
      e.payment_method,
      e.card_type,
      e.channel,
      e.transaction_type,
      e.country_code,
      e.transaction_ts,
      r.response AS raw_response
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
    ) AS r;
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_flink_statement.create_fraud_scorer,
    confluent_flink_statement.create_enriched_context,
    confluent_flink_statement.create_enriched_scored,
    confluent_flink_statement.insert_sanction_passed,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# DML: Stage 3b — Route scored payments (plain SQL, no async correlate)
#
# Reads from enriched_scored, parses JSON, and routes:
#   risk_score > 0.8  → payments_flagged  (HIGH_RISK_SCORE)
#   risk_score <= 0.8 → payments_cleared
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "insert_risk_scorer_flagged" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-risk-flagged-${local.random_id}"

  statement = <<-SQL
    INSERT INTO `payments_flagged`
    SELECT
      payment_id, customer_id, customer_name, merchant_name, merchant_category,
      amount, payment_method, card_type, channel, transaction_type,
      country_code, transaction_ts,
      'HIGH_RISK_SCORE' AS flag_reason
    FROM `enriched_scored`
    WHERE COALESCE(CAST(JSON_VALUE(raw_response, '$.risk_score') AS DOUBLE), -1.0) > 0.8;
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_flink_statement.create_enriched_scored,
    confluent_flink_statement.create_payments_flagged,
    confluent_flink_statement.insert_ml_predict,
  ]
}

resource "confluent_flink_statement" "insert_risk_scorer_cleared" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-risk-cleared-${local.random_id}"

  statement = <<-SQL
    INSERT INTO `payments_cleared`
    SELECT
      payment_id,
      customer_id,
      amount,
      transaction_type,
      transaction_ts,
      COALESCE(CAST(JSON_VALUE(raw_response, '$.risk_score') AS DOUBLE), -1.0) AS risk_score,
      COALESCE(JSON_VALUE(raw_response, '$.risk_tier'), 'PARSE_ERROR')          AS risk_tier,
      JSON_VALUE(raw_response, '$.reason')                                       AS reason
    FROM `enriched_scored`
    WHERE COALESCE(CAST(JSON_VALUE(raw_response, '$.risk_score') AS DOUBLE), -1.0) <= 0.8;
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_flink_statement.create_enriched_scored,
    confluent_flink_statement.create_payments_cleared,
    confluent_flink_statement.insert_ml_predict,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# DML: Stage 4 — Autonomous Resolver
#
# Calls AI_RUN_AGENT via LATERAL TABLE.
# RESOLVED → payments_cleared; ESCALATED or error → compliance_escalations.
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "insert_resolver_cleared" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-insert-resolver-cleared-${local.random_id}"

  statement = <<-SQL
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
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_flink_statement.create_fraud_resolver_agent,
    confluent_flink_statement.create_payments_flagged,
    confluent_flink_statement.create_payments_cleared,
    confluent_flink_statement.insert_sanction_flagged,
    confluent_flink_statement.insert_risk_scorer_flagged,
  ]
}

resource "confluent_flink_statement" "insert_resolver_escalated" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "pfp-insert-resolver-escalated-${local.random_id}"

  statement = <<-SQL
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
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }

  depends_on = [
    confluent_flink_statement.create_fraud_resolver_agent,
    confluent_flink_statement.create_payments_flagged,
    confluent_flink_statement.create_compliance_escalations,
    confluent_flink_statement.insert_sanction_flagged,
    confluent_flink_statement.insert_risk_scorer_flagged,
  ]
}
