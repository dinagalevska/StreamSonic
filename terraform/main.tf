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
    }
  }

  lifecycle {
  ignore_changes = [metadata["ssh-keys"]]
  }

}

output "kafka_vm_ip" {
  value = google_compute_instance.kafka_vm_instance.network_interface[0].access_config[0].nat_ip
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
          "--kafkaBrokerList", "35.239.250.116:9092"
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
