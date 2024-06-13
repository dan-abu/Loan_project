# Defining AWS provider
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Define AWS region
provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "lending_project" {
  bucket = var.lending_bucket_name
}

# Creating s3 folders
resource "aws_s3_object" "lending-project-landing" {
  bucket = aws_s3_bucket.lending_project.id
  key    = var.landing
}

resource "aws_s3_object" "lending-project-cleansed" {
  bucket = aws_s3_bucket.lending_project.id
  key    = var.cleansed
}

resource "aws_s3_object" "lending-project-review" {
  bucket = aws_s3_bucket.lending_project.id
  key    = var.review
}