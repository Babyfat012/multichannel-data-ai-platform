# Budget tùy chọn theo project, cảnh báo chi phí trước credit; không chặn chi tiêu.
data "google_project" "budget" {
  count      = var.enable_budget ? 1 : 0
  project_id = var.project_id
}

resource "google_billing_budget" "de" {
  count           = var.enable_budget ? 1 : 0
  billing_account = var.billing_account_id
  display_name    = "${var.name_prefix}-de-monthly"

  budget_filter {
    projects               = ["projects/${data.google_project.budget[0].number}"]
    credit_types_treatment = "EXCLUDE_ALL_CREDITS"
  }
  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.budget_amount_usd)
    }
  }
  threshold_rules {
    threshold_percent = 0.5
    spend_basis       = "CURRENT_SPEND"
  }
  threshold_rules {
    threshold_percent = 0.9
    spend_basis       = "CURRENT_SPEND"
  }
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }
  # Không khai báo all_updates_rule: khối này đòi Pub/Sub hoặc monitoring channel. Bỏ đi thì
  # Google vẫn gửi email mặc định cho Billing Admin/User — đủ cho cảnh báo POC.

  lifecycle {
    precondition {
      condition     = var.billing_account_id != null
      error_message = "enable_budget=true cần billing_account_id và quyền billing budget tương ứng."
    }
  }
  depends_on = [google_project_service.required]
}
