variable "instance_count" {}
variable "region" {}
variable "amis" {}
variable "instance_type" {}

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

    vpc_security_group_ids = ["${aws_security_group.allow_all.id}"]

    tags = {
        Name    = "r2t2-memcached-server"
        Project = "r2t2"
    }
}

output "ip_addresses" {
    value = "${aws_instance.memcached.*.public_ip}"
}
