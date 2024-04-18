variable "credentials" {
  default = "./service-account.json"
}

variable "project" {
  type    = string
  default = "zoomcamp-ady"
}

variable "region" {
  type    = string
  default = "asia-southeast2"
}

variable "gcs_bucket_name" {
  type    = string
  default = "event_zoomcamp_bucket"
}

variable "location" {
  type    = string
  default = "asia-southeast2"
}

variable "bq_dataset_name" {
  type    = string
  default = "event_zoomcamp_dataset"
}
