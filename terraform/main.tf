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