# Pod Identity tách quyền ứng dụng khỏi role node; không tạo AWS access key tĩnh.
data "aws_iam_policy_document" "pod_trust" {
  for_each = {
    ebs            = { namespace = "kube-system", service_account = "ebs-csi-controller-sa" }
    feature_reader = { namespace = "feature-store", service_account = "feature-api" }
  }
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/eks-cluster-arn"
      values   = [aws_eks_cluster.apps.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/kubernetes-namespace"
      values   = [each.value.namespace]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/kubernetes-service-account"
      values   = [each.value.service_account]
    }
  }
}

resource "aws_iam_role" "ebs" {
  name               = "${var.name_prefix}-eks-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.pod_trust["ebs"].json
}

resource "aws_iam_role_policy_attachment" "ebs" {
  role       = aws_iam_role.ebs.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEBSCSIDriverPolicyV2"
}

resource "aws_eks_pod_identity_association" "ebs" {
  cluster_name    = aws_eks_cluster.apps.name
  namespace       = "kube-system"
  service_account = "ebs-csi-controller-sa"
  role_arn        = aws_iam_role.ebs.arn
  depends_on      = [aws_eks_addon.pod_identity]
}

resource "aws_eks_addon" "ebs" {
  cluster_name = aws_eks_cluster.apps.name
  addon_name   = "aws-ebs-csi-driver"
  depends_on   = [aws_eks_pod_identity_association.ebs, aws_iam_role_policy_attachment.ebs]
}

resource "aws_iam_role" "feature_reader" {
  name               = "${var.name_prefix}-feature-reader"
  assume_role_policy = data.aws_iam_policy_document.pod_trust["feature_reader"].json
}

data "aws_iam_policy_document" "feature_reader" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${data.terraform_remote_state.data.outputs.lakehouse_bucket}/feature-store/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${data.terraform_remote_state.data.outputs.lakehouse_bucket}"]
  }
}

resource "aws_iam_role_policy" "feature_reader" {
  name   = "feature-store-read"
  role   = aws_iam_role.feature_reader.id
  policy = data.aws_iam_policy_document.feature_reader.json
}

resource "aws_eks_pod_identity_association" "feature_api" {
  cluster_name    = aws_eks_cluster.apps.name
  namespace       = "feature-store"
  service_account = "feature-api"
  role_arn        = aws_iam_role.feature_reader.arn
  depends_on      = [aws_eks_addon.pod_identity]
}
