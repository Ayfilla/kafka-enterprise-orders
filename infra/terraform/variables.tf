variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "db_username" {
  type      = string
  sensitive = true
}

variable "db_password" {
  type      = string
  sensitive = true
}

variable "kafka_bootstrap" {
  description = "Kafka bootstrap server"
  type        = string
  default     = "kafka:9092"
}

