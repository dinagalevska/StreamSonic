terraform {
  required_version = ">=1.0"
  backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  credentials = file(var.auth_key)
  project     = var.project_id
  region      = var.region
  zone        = var.zone
}

resource "google_compute_firewall" "port_rules" {
  project     = var.project_id
  name        = "kafka-broker-port"
  network     = "default"
  description = "Opens port 9092 in the Kafka VM for Spark cluster to connect"

  allow {
    protocol = "tcp"
    ports    = ["9092"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka"]
}

resource "google_compute_instance" "kafka_vm_instance" {
  name                      = "Musicdata-Streaming-Pipeline-kafka-instance"
  machine_type              = "e2-standard-2"
  tags                      = ["kafka"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-jammy-v20240904"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }
}

resource "google_compute_instance" "airflow_vm_instance" {
  name                      = "Musicdata-Streaming-Pipeline-airflow-instance"
  machine_type              = "e2-standard-2"
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-jammy-v20240904"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  lifecycle {
    ignore_changes = [metadata["ssh-keys"]]
  }

  service_account {
    email  = var.service_account_email
    scopes = ["cloud-platform"]
  }
}

output "airflow_vm_external_ip" {
  value       = google_compute_instance.airflow_vm_instance.network_interface[0].access_config[0].nat_ip
  description = "External IP address of the Airflow VM instance"
}

resource "google_storage_bucket" "streamsonic_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }
}

output "bucket_name" {
  value       = google_storage_bucket.streamsonic_bucket.name
  description = "Name of the Google Cloud Storage bucket created for data storage"
}

resource "google_dataproc_cluster" "mulitnode_spark_cluster" {
  name   = "Musicdata-Streaming-Pipeline-multinode-spark-cluster"
  region = var.region

  cluster_config {
    endpoint_config {
      enable_http_port_access = true
    }
    gce_cluster_config {
      network = "default"
      zone    = var.zone

      internal_ip_only = false
      service_account  = var.service_account_email
      service_account_scopes = [
        "cloud-platform"
      ]

      shielded_instance_config {
        enable_secure_boot = true
      }
    }

    lifecycle_config {
      idle_delete_ttl = "7200s" # 8 hours in seconds
    }

    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
    }

    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"
    }
  }
}

output "dataproc_cluster_name" {
  value       = google_dataproc_cluster.mulitnode_spark_cluster.name
  description = "Name of the Dataproc cluster for Spark processing"
}

output "dataproc_cluster_master_type" {
  value       = google_dataproc_cluster.mulitnode_spark_cluster.cluster_config[0].master_config[0].machine_type
  description = "Machine type of the Dataproc cluster master node"
}

resource "google_bigquery_dataset" "streamsonic_dataset" {
  dataset_id = var.dataset_id
  project    = var.project_id
  location   = var.region
}

