variable "credentials" {
  default = "./service-account.json"
}

variable "project" {
  type    = string
  default = "your Project Id"
}

variable "region" {
  type    = string
  default = "US"
}

variable "gcs_bucket_name" {
  type    = string
  default = "user_activities_bucket"
}

variable "location" {
  type    = string
  default = "US"
}

variable "bq_dataset_name" {
  type    = string
  default = "event_data_dataset"
}
