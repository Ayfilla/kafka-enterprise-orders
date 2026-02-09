variable "project" { type = string }

variable "region" { type = string }

variable "vpc_cidr" { type = string }

variable "azs" {
  type = list(string)
}

variable "public_subnets" {
  type = list(string)
}

variable "private_subnets" {
  type = list(string)
}

variable "db_username" { type = string }
variable "db_password" { type = string }

