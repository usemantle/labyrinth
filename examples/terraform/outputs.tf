output "aws_profile" {
  value = var.aws_profile
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "region" {
  value = var.aws_region
}

output "ecr_repository_url" {
  value = aws_ecr_repository.app.repository_url
}

output "rds_endpoint" {
  value = aws_db_instance.postgres.address
}

output "s3_bucket_name" {
  value = aws_s3_bucket.data.id
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.main.name
}

output "vpc_id" {
  value = aws_vpc.main.id
}

output "labyrinth_target_config" {
  description = "TOML snippet to paste into labyrinth project config"
  value       = <<-EOT
    [[targets]]
    urn = "urn:aws:account:${data.aws_caller_identity.current.account_id}:${var.aws_region}:root"
    plugins = ["s3", "rds", "ecr", "ecs", "vpc", "iam"]

    [targets.credentials]
    type = "aws_profile"
    profile = "${var.aws_profile}"
  EOT
}
