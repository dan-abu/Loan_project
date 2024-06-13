# Define variables
variable "aws_region" {
  description = "AWS region to create resources"
  default     = "eu-west-2"
}

variable "lending_bucket_name" {
  description = "Name of this project's S3 bucket"
  default     = "lending-project"
}

variable "landing" {
  description = "Landing bucket name"
  default     = "landing/" 
}

variable "cleansed" {
  description = "Cleansed bucket name"
  default     = "cleansed/" 
}

variable "review" {
  description = "Review bucket name"
  default     = "review/" 
}