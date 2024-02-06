terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  # Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  # credentials = file(var.credentials)
  project     = "data-engr-zoomcamp"
  region      = "us-central"
}

# resource "google_compute_network" "vpc_network" {
#   name = "terraform-network"
# }


resource "google_storage_bucket" "demo-bucket" {
  name          = "data-engr-zoomcamp_terabuck" #this is the bucket name and it must be unique 
  location      = "US"

  # # Optional, but recommended settings:
  # storage_class = "STANDARD"
  # uniform_bucket_level_access = true
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 3 // days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}



resource "google_bigquery_dataset" "demo_dataset" {
  # project    = "<Your Project ID>"
  dataset_id = "dataset_big"
  location   = "US"
}





# --------------------------------------------------------------------------------------------------------
# ### Execution 
# # Refresh service-account's auth-token for this session
# gcloud auth application-default login

# # Initialize state file (.tfstate)
# terraform init

# # Check changes to new infra plan
# terraform plan -var="project=<your-gcp-project-id>"


# # Create new infra
# terraform apply -var="project=<your-gcp-project-id>"