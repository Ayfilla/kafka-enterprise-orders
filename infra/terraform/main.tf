terraform {
  required_version = ">= 1.5"

  backend "local" {
    path = "terraform.tfstate"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

module "vpc" {
  source = "./vpc"

  vpc_cidr        = var.vpc_cidr
  azs             = var.azs
  public_subnets  = var.public_subnets
  private_subnets = var.private_subnets
}

module "rds" {
  source = "./rds"

  vpc_id     = module.vpc.vpc_id
  subnets    = module.vpc.private_subnets_ids
  username   = var.db_username
  password   = var.db_password
}

module "ecs" {
  source = "./ecs"

  vpc_id     = module.vpc.vpc_id
  subnets    = module.vpc.public_subnets_ids
}

