# ──────────────────────────────────────────────────────────────────
# Route53 Hosted Zone (existing)
# ──────────────────────────────────────────────────────────────────

data "aws_route53_zone" "main" {
  name         = var.hosted_zone_name
  private_zone = false
}

# ──────────────────────────────────────────────────────────────────
# ACM Certificate for API Gateway custom domain
# ──────────────────────────────────────────────────────────────────

resource "aws_acm_certificate" "api" {
  domain_name       = "api.${var.hosted_zone_name}"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = { Name = "${var.project_name}-api-cert" }
}

resource "aws_route53_record" "acm_validation" {
  for_each = {
    for dvo in aws_acm_certificate.api.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  zone_id = data.aws_route53_zone.main.zone_id
  name    = each.value.name
  type    = each.value.type
  records = [each.value.record]
  ttl     = 60

  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "api" {
  certificate_arn         = aws_acm_certificate.api.arn
  validation_record_fqdns = [for record in aws_route53_record.acm_validation : record.fqdn]
}

# ──────────────────────────────────────────────────────────────────
# DNS Records
# ──────────────────────────────────────────────────────────────────

resource "aws_route53_record" "api" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = "api.${var.hosted_zone_name}"
  type    = "A"

  alias {
    name                   = aws_apigatewayv2_domain_name.app.domain_name_configuration[0].target_domain_name
    zone_id                = aws_apigatewayv2_domain_name.app.domain_name_configuration[0].hosted_zone_id
    evaluate_target_health = false
  }
}
