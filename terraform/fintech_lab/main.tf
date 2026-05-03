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
# Flink DDL: payments (faker data generator)
#
# Streams synthetic payment events using the Flink faker connector.
# - 50 fixed customer IDs so each customer accumulates history quickly
# - amount: 191 normal values ($12-$110) + 1 anomalous spike ($8,750) ~0.5%
# - transaction_ts declared as event-time attribute via WATERMARK
# ─────────────────────────────────────────────────────────────────────────────
resource "confluent_flink_statement" "create_payments" {
  organization { id = local.organization_id }
  environment  { id = local.environment_id }
  compute_pool { id = local.compute_pool_id }
  principal    { id = local.service_account_id }

  statement_name = "fintech-lab-create-payments-${local.random_id}"

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS `payments` (
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
      WATERMARK FOR `transaction_ts` AS `transaction_ts` - INTERVAL '5' SECOND
    ) WITH (
      'connector'       = 'faker',
      'rows-per-second' = '10',
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
      'fields.transaction_ts.expression'    = '#{date.past ''5'',''SECONDS''}'
    );
  SQL

  properties    = local.flink_properties
  rest_endpoint = local.flink_rest_endpoint
  credentials {
    key    = local.flink_api_key
    secret = local.flink_api_secret
  }
}
