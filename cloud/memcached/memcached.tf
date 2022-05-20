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

resource "aws_security_group" "allow_all" {
    name = "allow_all"
    description = "Allow all traffic"

    ingress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Project = "r2t2"
    }
}

resource "aws_instance" "memcached" {
    count = var.instance_count

    ami = var.amis[var.region]
    instance_type = var.instance_type

    user_data = <<-EOT
        #!/bin/bash
        echo "N=${var.servers_per_instance}" >/home/ubuntu/memcached-servers.env
        echo "THREADS=${var.threads_per_server}" >>/home/ubuntu/memcached-servers.env
        echo "MEMORY=${var.memory_per_server}" >>/home/ubuntu/memcached-servers.env
        echo "MAXOBJ=${var.max_object_size}" >>/home/ubuntu/memcached-servers.env
        echo "FLAGS=${var.additional_flags}" >>/home/ubuntu/memcached-servers.env
        chmod 644 /home/ubuntu/memcached-servers.env
        systemctl restart r2t2-memcached.service
        EOT

    vpc_security_group_ids = ["${aws_security_group.allow_all.id}"]

    tags = {
        Name    = "r2t2-memcached-server"
        Project = "r2t2"
    }
}

output "ip_addresses" {
    value = "${aws_instance.memcached.*.public_ip}"
}

output "servers_per_instance" {
    value = "${var.servers_per_instance}"
}
