# Budget tùy chọn: cảnh báo chi phí thật trước credit; không chặn chi tiêu.
resource "aws_budgets_budget" "data_platform" {
  count        = var.enable_budget ? 1 : 0
  name         = "${var.name_prefix}-data-platform-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_amount_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Không trừ credit: thấy đúng số tiền đang đốt, giống EXCLUDE_ALL_CREDITS bên GCP.
  cost_types {
    include_credit = false
  }

  dynamic "notification" {
    for_each = [50, 90, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.budget_email]
    }
  }

  lifecycle {
    precondition {
      condition     = var.budget_email != null
      error_message = "enable_budget=true cần budget_email; AWS không tự gửi cảnh báo cho ai."
    }
  }
}
