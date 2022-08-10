#!/bin/bash -exu

# https://www.packer.io/docs/debugging#issues-installing-ubuntu-packages
cloud-init status --wait

MEMCACHED_SERVICE=r2e2-memcached.service
MEMCACHED_RUNNER=/usr/local/bin/r2e2-memcached-runner
MEMCACHED_ENV=/home/ubuntu/r2e2-memcached.env

sudo apt-get update && sudo apt-get upgrade
sudo apt-get install -y memcached

# disable the default memcached service
sudo systemctl daemon-reload
sudo systemctl stop memcached || true
sudo systemctl mask memcached

# WRITING OUT THE SETTINGS
cat <<'EOF' | tee ${MEMCACHED_ENV}
N=1
THREADS=2
MEMORY=5120
MAXOBJ=4m
FLAGS=--disable-cas
EOF

sudo chmod 644 ${MEMCACHED_ENV}

# WRITING OUT THE MEMCACHED SCRIPT
cat <<'EOF' | sudo tee ${MEMCACHED_RUNNER}
#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

for port in $(seq 11211 $((${N:?} + 11210)))
do
  memcached -v -m ${MEMORY:?} -b 4096 -c 8192 -t ${THREADS:?} -M -I ${MAXOBJ:?} -l 0.0.0.0 -p ${port} ${FLAGS} &
done

wait
EOF

sudo chmod +x ${MEMCACHED_RUNNER}

# WRITING OUT MEMCACHED SERVICE
cat <<EOF | sudo tee /etc/systemd/system/${MEMCACHED_SERVICE}
[Unit]
Description=R2E2 memcached service

[Service]
Type=simple
ExecStart=${MEMCACHED_RUNNER}
EnvironmentFile=${MEMCACHED_ENV}
User=ubuntu
Group=ubuntu

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl start ${MEMCACHED_SERVICE}
sudo systemctl enable ${MEMCACHED_SERVICE}
