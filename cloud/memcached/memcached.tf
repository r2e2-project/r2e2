variable "instance_count" {}
variable "region" {}
variable "amis" {}
variable "instance_type" {}

variable "servers_per_instance" {}
variable "threads_per_server" {}
variable "memory_per_server" {}
variable "max_object_size" {}
variable "additional_flags" {}

provider "aws" {
    profile = "default"
    region = var.region
}

resource "aws_security_group" "r2e2_allow_all" {
    name = "allow_all"
    description = "Allow all traffic"

    ingress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Project = "r2e2"
    }
}

resource "aws_instance" "memcached" {
    count = var.instance_count

    ami = var.amis[var.region]
    instance_type = var.instance_type

    user_data = <<-EOT
        #!/bin/bash
        echo "N=${var.servers_per_instance}" >/home/ubuntu/r2e2-memcached.env
        echo "THREADS=${var.threads_per_server}" >>/home/ubuntu/r2e2-memcached.env
        echo "MEMORY=${var.memory_per_server}" >>/home/ubuntu/r2e2-memcached.env
        echo "MAXOBJ=${var.max_object_size}" >>/home/ubuntu/r2e2-memcached.env
        echo "FLAGS=${var.additional_flags}" >>/home/ubuntu/r2e2-memcached.env
        chmod 644 /home/ubuntu/r2e2-memcached.env
        systemctl restart r2e2-memcached.service
        EOT

    vpc_security_group_ids = ["${aws_security_group.r2e2_allow_all.id}"]

    tags = {
        Name    = "r2e2-memcached-server"
        Project = "r2e2"
    }
}

output "ip_addresses" {
    value = "${aws_instance.memcached.*.public_ip}"
}

output "servers_per_instance" {
    value = "${var.servers_per_instance}"
}
