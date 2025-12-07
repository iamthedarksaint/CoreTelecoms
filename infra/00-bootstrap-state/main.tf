provider "aws" {
  region = "eu-north-1"
  profile = "destination"
}


resource aws_s3_bucket "terraform_state" {
    bucket = "core-telecoms-hassan-terraform-state"

}