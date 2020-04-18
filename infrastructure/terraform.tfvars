instance_count = 2
region = "us-west-2"

amis = {
    "us-west-1" = "ami-03c5b55b809ce25f3"
    "us-west-2" = "ami-0d11e47ca7049df43"
}

servers_per_instance = 1
threads_per_server = 2
memory_per_server = 4096

instance_type = "c5n.large"
