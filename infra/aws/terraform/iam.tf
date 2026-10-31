# VM nhận credential tạm qua instance profile, chỉ có quyền dữ liệu trên hai bucket; không tạo access key.
data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "vm" {
  name               = "${var.name_prefix}-data-platform-vm"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

data "aws_iam_policy_document" "buckets" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.data : b.arn]
  }
  statement {
    sid       = "ObjectReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [for b in aws_s3_bucket.data : "${b.arn}/*"]
  }
}

resource "aws_iam_role_policy" "buckets" {
  name   = "platform-buckets"
  role   = aws_iam_role.vm.id
  policy = data.aws_iam_policy_document.buckets.json
}

# Policy có sẵn của AWS cho SSM agent; thay vai IAP + OS Login bên GCP.
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.vm.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Lớp "vỏ" để gắn role vào EC2; GCP không có lớp này.
resource "aws_iam_instance_profile" "vm" {
  name = "${var.name_prefix}-data-platform-vm"
  role = aws_iam_role.vm.name
}

# Jenkins trên EC2 (đợt 5) push/pull image vào đúng các repo ECR của dự án; không xoá repo/image.
data "aws_iam_policy_document" "ci_ecr" {
  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    sid = "EcrPushPull"
    actions = [
      "ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer",
      "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage",
      "ecr:DescribeImages", "ecr:DescribeRepositories",
    ]
    resources = [for repo in aws_ecr_repository.application : repo.arn]
  }
  # `aws eks update-kubeconfig` cần DescribeCluster; quyền trong cluster vẫn do access entry `ci` + RBAC quyết.
  statement {
    sid       = "EksDescribe"
    actions   = ["eks:DescribeCluster"]
    resources = ["arn:aws:eks:${var.region}:${data.aws_caller_identity.current.account_id}:cluster/${var.name_prefix}-apps"]
  }
}

resource "aws_iam_role_policy" "ci_ecr" {
  count  = var.enable_ecr ? 1 : 0
  name   = "platform-ci-ecr"
  role   = aws_iam_role.vm.id
  policy = data.aws_iam_policy_document.ci_ecr.json
}
