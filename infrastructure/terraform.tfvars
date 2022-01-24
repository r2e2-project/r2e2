instance_count = 2
region = "us-west-2"

amis = {
    "us-west-1" = "ami-08b094f66ffbbfc1a"
    "us-west-2" = "ami-0d8e9cadaed00a4f4"
}

servers_per_instance = 1
threads_per_server = 2
memory_per_server = 5120
max_object_size = "1m"
additional_flags = "--disable-cas"

instance_type = "c5n.large"
