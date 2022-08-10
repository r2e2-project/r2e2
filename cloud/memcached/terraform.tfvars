instance_count = 2
region = "us-west-2"

amis = {
    "us-west-1" = "ami-0af8615fa1c76960b"
    "us-west-2" = "ami-055713a4c1d750c02"
}

servers_per_instance = 1
threads_per_server = 2
memory_per_server = 5120
max_object_size = "4m"
additional_flags = "--disable-cas"

instance_type = "c5n.large"
