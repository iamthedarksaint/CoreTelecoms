  terraform {
    backend "s3" {
    bucket         = "core-telecoms-hassan-terraform-state"
    key            = "data-platform/terraform.tfstate"
    region         = "eu-north-1"
    encrypt        = true
    use_lockfile   = true 
    profile        = "destination"
  }
  }
 