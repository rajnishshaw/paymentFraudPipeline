# ── DDL statement names ──────────────────────────────────────────────────────
output "ddl_payments_flagged_statement" {
  value = confluent_flink_statement.create_payments_flagged.statement_name
}
output "ddl_enriched_context_statement" {
  value = confluent_flink_statement.create_enriched_context.statement_name
}
output "ddl_payments_cleared_statement" {
  value = confluent_flink_statement.create_payments_cleared.statement_name
}
output "ddl_compliance_escalations_statement" {
  value = confluent_flink_statement.create_compliance_escalations.statement_name
}
output "ddl_payments_with_history_statement" {
  value = confluent_flink_statement.create_payments_with_history.statement_name
}
output "ddl_enriched_scored_statement" {
  value = confluent_flink_statement.create_enriched_scored.statement_name
}
output "ddl_openai_conn_statement" {
  value = confluent_flink_statement.create_openai_conn.statement_name
}
output "ddl_fraud_scorer_statement" {
  value = confluent_flink_statement.create_fraud_scorer.statement_name
}
output "ddl_fraud_resolver_agent_statement" {
  value = confluent_flink_statement.create_fraud_resolver_agent.statement_name
}

# ── Stage 1: Sanction Filter ──────────────────────────────────────────────────
output "stage1_sanction_flagged_statement" {
  value = confluent_flink_statement.insert_sanction_flagged.statement_name
}
# Stage 1 pass-through writes directly to enriched_context (no separate job)
output "stage1_sanction_passed_statement" {
  value = confluent_flink_statement.insert_sanction_passed.statement_name
}

# ── Stage 3: Risk Scorer ──────────────────────────────────────────────────────
output "stage3_ml_predict_statement" {
  value = confluent_flink_statement.insert_ml_predict.statement_name
}
output "stage3_risk_scorer_flagged_statement" {
  value = confluent_flink_statement.insert_risk_scorer_flagged.statement_name
}
output "stage3_risk_scorer_cleared_statement" {
  value = confluent_flink_statement.insert_risk_scorer_cleared.statement_name
}

# ── Stage 4: Autonomous Resolver ──────────────────────────────────────────────
output "stage4_resolver_cleared_statement" {
  value = confluent_flink_statement.insert_resolver_cleared.statement_name
}
output "stage4_resolver_escalated_statement" {
  value = confluent_flink_statement.insert_resolver_escalated.statement_name
}
