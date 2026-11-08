#!/bin/bash

# Skip if the matching MLNX_OFED version is already installed.
if command -v ofed_info >/dev/null 2>&1 && ofed_info -s | grep -q "MLNX_OFED_LINUX-4.9-5.1.0.0"; then
  echo "MLNX_OFED ${MLNX_VERSION} already installed, skipping."
  exit 0
fi

wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz
tar -xvf MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz
cd MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64

sudo ./mlnxofedinstall  --force
sudo /etc/init.d/openibd restart
sudo /etc/init.d/opensmd restart

cd ..
sudo rm -rf ./MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64
sudo rm ./MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz