variable "auth_key" {
  default     = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/keys/streamsonic-441414-1c56c1f4d03c.json"
  description = "GCP Serivice Account Authentication Key"
}

variable "project_id" {
  default     = "streamsonic-441414"
  description = "GCP Project ID"
}

variable "region" {
  default     = "us-central1"
  description = "Region for resources"
}

variable "zone" {
  default     = "us-central1-a"
  description = "Location zone"
}

variable "service_account_email" {
  default     = "terraform-admin@streamsonic-441414.iam.gserviceaccount.com"
  description = "GCP Service Account"
}

# variable "bucket_name" {
#   default     = "streamsonic_bucket"
#   description = "Name of the Google Cloud Storage bucket for data storage"
# }