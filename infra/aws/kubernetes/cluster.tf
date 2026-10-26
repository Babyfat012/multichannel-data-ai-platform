# Cluster API private cho node; endpoint public chỉ nhận CIDR quản trị đã khai báo.
data "aws_iam_policy_document" "cluster_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.name_prefix}-eks-control-plane"
  assume_role_policy = data.aws_iam_policy_document.cluster_trust.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "apps" {
  name     = "${var.name_prefix}-apps"
  role_arn = aws_iam_role.cluster.arn
  version  = var.kubernetes_version
  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = false
  }
  vpc_config {
    subnet_ids              = data.terraform_remote_state.data.outputs.kubernetes_subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.api_allowed_cidrs
  }
  enabled_cluster_log_types = ["api", "audit", "authenticator"]
  depends_on                = [aws_iam_role_policy_attachment.cluster]
  lifecycle {
    precondition {
      condition = (
        data.terraform_remote_state.data.outputs.region == var.region &&
        length(data.terraform_remote_state.data.outputs.kubernetes_subnet_ids) >= 2
      )
      error_message = "Root Data Platform phải cùng region, đã apply enable_kubernetes_network với hai subnet khác AZ."
    }
  }
}

resource "aws_eks_access_entry" "admin" {
  cluster_name  = aws_eks_cluster.apps.name
  principal_arn = var.admin_role_arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "admin" {
  cluster_name  = aws_eks_cluster.apps.name
  principal_arn = aws_eks_access_entry.admin.principal_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  access_scope { type = "cluster" }
}

resource "aws_vpc_security_group_ingress_rule" "data_services" {
  for_each                     = { for port in var.data_service_ports : tostring(port) => port }
  security_group_id            = data.terraform_remote_state.data.outputs.data_security_group_id
  referenced_security_group_id = aws_eks_cluster.apps.vpc_config[0].cluster_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = each.value
  to_port                      = each.value
  description                  = "EKS workloads to Data Platform private endpoint"
}
