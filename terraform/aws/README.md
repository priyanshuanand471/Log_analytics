# Optional AWS CLI + Terraform exercise
This folder demonstrates Infrastructure as Code by creating a **private S3 bucket only**. It does **not** deploy the Docker pipeline to AWS.

Requires AWS CLI, Terraform, configured AWS credentials, and an AWS account. AWS resources may incur costs.

```bash
aws sts get-caller-identity
terraform init
terraform plan
terraform apply
terraform destroy
```

Never commit AWS access keys. Run `terraform destroy` when finished to avoid leaving chargeable resources. Terraform state may contain sensitive information; store it securely.
