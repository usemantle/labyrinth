variable "aws_profile" {
  description = "AWS CLI profile to use for authentication"
  type        = string
  default     = "test"
}

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name prefix for all resources"
  type        = string
  default     = "labyrinth-test"
}

variable "db_password" {
  description = "Password for the RDS database"
  type        = string
  sensitive   = true
}

variable "hosted_zone_name" {
  description = "Existing Route53 hosted zone to create records in"
  type        = string
}
