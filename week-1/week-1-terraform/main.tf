terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  credentials = file("keys/de-zoomcamp-501902-b520090ca660.json")
  project     = "de-zoomcamp-501902"
  region      = "us-central1"
  zone        = "us-central1-c"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "de-zoomcamp-501902-bucket"
  location      = "US"
  force_destroy = true

}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id                  = var.bigquery_dataset_id
  
}