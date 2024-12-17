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

resource "google_compute_address" "kafka_static_ip" {
  name   = "kafka-static-ip"
  region = var.region
}

resource "google_compute_instance" "kafka_vm_instance" {
  name                      = "musicdata-streaming-pipeline-kafka-instance"
  machine_type              = "e2-standard-2"
  zone                      = var.zone
  tags                      = ["kafka"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-jammy-v20240904"
      size = 30
    }
  }

  network_interface {
    network = "default"
    access_config {
            nat_ip = google_compute_address.kafka_static_ip.address  
    }
  }

  lifecycle {
  ignore_changes = [metadata["ssh-keys"]]
  }

}

output "kafka_vm_ip" {
  value = google_compute_address.kafka_static_ip.address 
}

resource "google_cloud_run_v2_job" "default" {
  name                = "data-generate-job"
  location            = var.region
  deletion_protection = false

  template {
    template {
      containers {
        image = "gcr.io/streamsonic-441414/streamsonic:latest"

        resources {
          limits = {
            "memory" = "4Gi"
          }
        }

        command = []

        args = [
          "-c", "examples/example-config.json",
          "--start-time", "2021-01-01T00:00:00",
          "--end-time", "2021-12-01T00:00:00",
          "--nusers", "100",
          "--kafkaBrokerList", "${google_compute_address.kafka_static_ip.address}:9092"
        ]
      }

      timeout = "600s" # Adjust the timeout as needed
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].template[0].containers[0].resources[0].limits
    ]
  }
}

resource "google_storage_bucket" "streamsonic_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  # versioning {
  #   enabled = true
  # }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 60 # days
    }
  }
}

output "bucket_name" {
  value       = google_storage_bucket.streamsonic_bucket.name
  description = "Name of the Google Cloud Storage bucket created for data storage"
}

resource "google_dataproc_cluster" "mulitnode_spark_cluster" {
  name   = "dataproc-cluster-musicdata-stream"
  region = var.region

  cluster_config {

    staging_bucket = var.bucket_name

    gce_cluster_config {
      network = "default"
      zone    = var.zone

      shielded_instance_config {
        enable_secure_boot = true
      }
    }

    master_config {
      num_instances = 1
      machine_type  = "e2-standard-2"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 30
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "e2-medium"
      disk_config {
        boot_disk_size_gb = 30
      }
    }

    software_config {
      image_version = "2.0-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
      optional_components = ["JUPYTER"]
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
  dataset_id = "streamsonic_dataset"
  project    = var.project_id
}

resource "google_compute_instance" "airflow_vm_instance" {
  name                      = "streamify-airflow-instance"
  machine_type              = "e2-standard-4"
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
      size  = 30
    } 
  }

  network_interface {
    network = "default"
    access_config {
    }
  }
}

output "airflow_vm_external_ip" {
  value = google_compute_instance.airflow_vm_instance.network_interface[0].access_config[0].nat_ip
}

