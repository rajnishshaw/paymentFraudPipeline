variable "openai_api_key" {
  description = "OpenAI API key used by the fraud_scorer model and fraud_resolver_agent"
  type        = string
  sensitive   = true
}
