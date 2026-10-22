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
  name               = "${var.name_prefix}-de-vm"
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
  name   = "fsds-buckets"
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
  name = "${var.name_prefix}-de-vm"
  role = aws_iam_role.vm.name
}
