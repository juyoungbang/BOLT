#!/bin/bash

# Helpers to skip already-installed libraries.
has_cmd() { command -v "$1" >/dev/null 2>&1; }
has_pkg() { dpkg -s "$1" >/dev/null 2>&1; }
has_file() { ls $1 >/dev/null 2>&1; }

# cmake
if has_cmd cmake; then
  echo "cmake already installed, skipping."
else
  wget https://cmake.org/files/v3.16/cmake-3.16.9.tar.gz
  tar -xvzf cmake-3.16.9.tar.gz
  cd ./cmake-3.16.9
  ./bootstrap --prefix=/usr/local
  make
  sudo make install
  cd ..

  sudo rm -rf ./cmake-3.16.9
  sudo rm -rf ./cmake-3.16.9.tar.gz
fi

# numa
if has_pkg libnuma-dev; then
  echo "libnuma-dev already installed, skipping."
else
  sudo apt-get -y --force-yes install libnuma-dev
fi

# cityhash
if has_file "/usr/local/include/city.h"; then
  echo "cityhash already installed, skipping."
else
  git clone https://github.com/google/cityhash.git
  cd cityhash
  ./configure
  make all check CXXFLAGS="-g -O3"
  sudo make install
  cd ..

  sudo rm -rf ./cityhash
fi

# boost
if has_file "/usr/local/include/boost/version.hpp"; then
  echo "boost already installed, skipping."
else
  wget https://jaist.dl.sourceforge.net/project/boost/boost/1.65.1/boost_1_65_1.tar.gz
  tar -xvf ./boost_1_65_1.tar.gz
  cd boost_1_65_1
  ./bootstrap.sh
  ./b2 install --with-system --with-coroutine --layout=versioned threading=multi
  sudo apt-get -y --force-yes install libboost-all-dev
  cd ..

  sudo rm -rf ./boost_1_65_1
  sudo rm -rf ./boost_1_65_1.tar.gz
fi

# tbb
if has_file "/usr/local/include/tbb/tbb.h"; then
  echo "tbb already installed, skipping."
else
  git clone https://github.com/wjakob/tbb.git
  cd tbb/build
  cmake ..
  make -j
  sudo make install
  cd ../..

  ldconfig
fi

# openjdk-8
if has_pkg openjdk-8-jdk; then
  echo "openjdk-8-jdk already installed, skipping."
else
  sudo apt-get -y --force-yes install openjdk-8-jdk
fi

# jq
if has_cmd jq; then
  echo "jq already installed, skipping."
else
  sudo apt-get install jq
fi

# mpstat
if has_cmd mpstat; then
  echo "sysstat already installed, skipping."
else
  sudo apt install sysstat
fi
