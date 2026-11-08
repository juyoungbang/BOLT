#ifndef _CONFIG_H
#define _CONFIG_H

#include <fstream>
#include <stdint.h>
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#define MASTER_SERVER_ID 0

struct ServerConfig {
  uint16_t sid;
  uint16_t udp_port;
  uint16_t server_num;
  uint16_t client_num;
  char server_ips[16][16];

  uint32_t ib_dev_id;
  uint32_t ib_port_id;
  uint8_t ib_gid_idx;
  bool is_roce;

  /* Memory server MR size. */
  uint64_t tab_size;
  uint64_t dir_size;
  uint64_t stash_size;
  uint64_t bucket_size;
  uint64_t kv_data_size;
  
  /* Local RDMA buffer size. */
  uint64_t base_size;
};

struct ClientConfig {
  uint16_t cid;
  uint16_t udp_port;
  uint16_t server_num;
  uint16_t client_num;
  char server_ips[16][16];
  char client_ips[16][16];

  uint32_t ib_dev_id;
  uint32_t ib_port_id;
  uint8_t ib_gid_idx;
  bool is_roce;

  uint64_t cache_size;
  uint64_t base_size;
};

static int load_server_config(const char* fname, struct ServerConfig* conf) {
  std::fstream config_fs(fname);

  boost::property_tree::ptree pt;
  try {
    boost::property_tree::read_json(config_fs, pt);
  } catch (boost::property_tree::ptree_error& e) {
    fprintf(stderr, "[ERROR] Failed reading configuration file (error: %s)\n", e.what());
    exit(1);
  }

  try {
    conf->sid = pt.get<uint16_t>("server_id");
    conf->udp_port = pt.get<uint16_t>("udp_port");
    conf->server_num = pt.get<uint16_t>("server_num");
    conf->client_num = pt.get<uint16_t>("client_num");

    assert(conf->server_num <= 16);

    int i = 0;
    BOOST_FOREACH(boost::property_tree::ptree::value_type& v, pt.get_child("server_ips")) {
      std::string ip = v.second.get<std::string>("");
      strcpy(conf->server_ips[i], ip.c_str());
      ++i;
    }

    assert(i == conf->server_num);
    
    conf->tab_size = pt.get<uint64_t>("tab_size", 9UL * 1024 * 1024 * 1024);
    conf->dir_size = pt.get<uint64_t>("dir_size", 5UL * 1024 * 1024 * 1024);
    conf->stash_size = pt.get<uint64_t>("stash_size", 1UL * 1024 * 1024 * 1024);
    conf->bucket_size = pt.get<uint64_t>("bucket_size", 14UL * 1024 * 1024 * 1024);
    conf->kv_data_size = pt.get<uint64_t>("kv_data_size", 220UL * 1024 * 1024 * 1024);

    conf->ib_dev_id = pt.get<uint32_t>("ib_dev_id");
    conf->ib_port_id = pt.get<uint32_t>("ib_port_id");
    conf->ib_gid_idx = pt.get<uint8_t>("ib_gid_idx");

    std::string is_roce = pt.get<std::string>("conn_type");
    conf->is_roce = strcmp(is_roce.c_str(), "RoCE") == 0 ? 1 : 0;

    conf->base_size = pt.get<uint64_t>("base_size", 1UL * 1024 * 1024 * 1024);
  } catch (boost::property_tree::ptree_error& e) {
    fprintf(stderr, "[ERROR] Failed reading configuration file (error: %s)\n", e.what());
    exit(1);
  }

  return 0;
}

static int load_client_config(const char* fname, struct ClientConfig* conf) {
  std::fstream config_fs(fname);

  boost::property_tree::ptree pt;
  try {
    boost::property_tree::read_json(config_fs, pt);
  } catch (boost::property_tree::ptree_error& e) {
    fprintf(stderr, "[ERROR] Failed reading configuration file (error: %s)\n", e.what());
    exit(1);
  }

  try {
    conf->cid = pt.get<uint16_t>("client_id");
    conf->udp_port = pt.get<uint16_t>("udp_port");
    conf->server_num = pt.get<uint16_t>("server_num");
    conf->client_num = pt.get<uint16_t>("client_num");

    assert(conf->server_num <= 16);
    assert(conf->client_num <= 16);

    int i = 0;
    BOOST_FOREACH(boost::property_tree::ptree::value_type& v, pt.get_child("server_ips")) {
      std::string ip = v.second.get<std::string>("");
      strcpy(conf->server_ips[i], ip.c_str());
      ++i;
    }

    assert(i == conf->server_num);

    i = 0;
    BOOST_FOREACH(boost::property_tree::ptree::value_type& v, pt.get_child("client_ips")) {
      std::string ip = v.second.get<std::string>("");
      strcpy(conf->client_ips[i], ip.c_str());
      ++i;
    }

    assert(i == conf->client_num);

    conf->ib_dev_id = pt.get<uint32_t>("ib_dev_id");
    conf->ib_port_id = pt.get<uint32_t>("ib_port_id");
    conf->ib_gid_idx = pt.get<uint8_t>("ib_gid_idx");

    std::string is_roce = pt.get<std::string>("conn_type");
    conf->is_roce = strcmp(is_roce.c_str(), "RoCE") == 0 ? 1 : 0;
    
    conf->cache_size = pt.get<uint64_t>("cache_size", 1UL * 1024 * 1024 * 1024);
    conf->base_size = pt.get<uint64_t>("base_size", 50UL * 1024 * 1024);
  } catch (boost::property_tree::ptree_error& e) {
    fprintf(stderr, "[ERROR] Failed reading configuration file (error: %s)\n", e.what());
    exit(1);
  }

  return 0;
}

#endif // _CONFIG_H