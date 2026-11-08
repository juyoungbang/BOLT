#include <random>
#include <vector>
#include <algorithm>
#include <fstream>
#include <string>

#include "zipf.h"

constexpr uint64_t KEY_MASK = (1UL << 56) - 1;

inline uint64_t str2int(const std::string& key) {
  uint64_t res = 0;
  uint64_t len = std::min(key.size(), sizeof(Key_t));
  memcpy(&res, key.data() + (key.size() - len), len);
  return res & KEY_MASK;
}

inline uint64_t str2int_xor(const std::string& key) {
  uint64_t i = 0;
  uint64_t res = 0;
  uint64_t len = key.size();

  // XOR-fold full 8-byte chunks
  for (; i + 8 <= len; i += 8) {
    uint64_t chunk = 0;
    memcpy(&chunk, key.data()+i, 8);
    res ^= chunk;
  }

  // XOR-fold tail bytes (0–7 bytes)
  if (i < len) {
    uint64_t chunk = 0;
    memcpy(&chunk, key.data()+i, len-i);
    res ^= chunk;
  }

  return res & KEY_MASK;
}

bool skip_BOM(std::ifstream& in) {
  char test[4] = {0};

  in.read(test, 3);
  if (strcmp(test, "\xEF\xBB\xBF") == 0) {
    return true;
  } 
  in.seekg(0);

  return false;
}

std::vector<Request> get_search_keys(std::vector<Request> array, int num_keys, int num_searches) {
  std::mt19937_64 gen(std::random_device{}());
  std::uniform_int_distribution<int> dis(0, num_keys - 1);
  auto keys = std::vector<Request>(num_searches);
  for (int i = 0; i < num_searches; i++) {
    int pos = dis(gen);
    assert(pos < num_searches);
    keys[i] = array[pos];
  }
  return keys;
}

std::vector<Request> get_search_keys(int seed, std::vector<Request> array, int num_keys, int num_searches) {
  std::mt19937_64 gen(seed);
  std::uniform_int_distribution<int> dis(0, num_keys - 1);
  auto keys = std::vector<Request>(num_searches);
  for (int i = 0; i < num_searches; i++) {
    int pos = dis(gen);
    assert(pos < num_searches);
    keys[i] = array[pos];
  }
  return keys;
}

std::vector<Request> get_search_keys_zipf(std::vector<Request> array, int num_keys, int num_searches) {
  auto keys = std::vector<Request>(num_searches);
  ScrambledZipfianGenerator zipf_gen(num_keys);
  for (int i = 0; i < num_searches; i++) {
    int pos = zipf_gen.nextValue();
    keys[i].key = array[pos].key;
  }
  return keys;
}

std::vector<Request> get_search_keys_zipf(int seed, std::vector<Request> array, int num_keys, int num_searches) {
  auto keys = std::vector<Request>(num_searches);
  ScrambledZipfianGenerator zipf_gen(seed, num_keys);
  for (int i = 0; i < num_searches; i++) {
    int pos = zipf_gen.nextValue();
    keys[i].key = array[pos].key;
  }
  return keys;
}

/* 
** [SEQ] Generate sorted keys and allocate it sequentially to clients.
**       e.g., { (1 2 3) , (4 5 6) } 
** [RAND] Generate sorted keys and allocate it randomly to clients. 
**        e.g., { (2 5 1) , (4 3 6) }
** [RAND_ALLOW_DUP] Generate duplicated keys and allocate it randomly to clients.
**                  e.g., { (2 5 5) , (2 3 9) }
** [RR] Generate sorted keys and allocate it in a round-robin manner to clients.
**      e.g., { (1 3 5) , (2 4 6) }
** [RR_SHUF] Generate sorted keys, allocate it in a round-robin manner to clients and shuffle within clients.
**           e.g., { (3 5 1) , (2 6 4) }
*/ 
enum KeyGenType {
  SEQ, 
  RAND,
  RAND_ALLOW_DUP,
  RR, 
  RR_SHUF 
};

enum KeyGenDistribution {
  UNIFORM
};

template<typename K, typename V>
class KeyGenerator {
private:
  std::random_device rd;

  std::vector<K>* gen_seq_keys(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid);
  std::vector<K>* gen_rand_keys(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid);
  std::vector<K>* gen_rand_dup_keys(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid);
  std::vector<K>* gen_rr_keys(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid, bool shuf);

public:
  V make_value(K key, bool random = false);
  void print_keys(std::vector<K>* keys, uint64_t num_threads, uint16_t num_coroutines);

  std::vector<K>* gen_key_single_client(std::vector<K> keys, uint64_t num_keys, uint64_t num_threads, uint16_t num_coroutines, bool shuf = false);

  /*
  ** num_keys       -> Total number of keys among all clients
  ** num_clients    -> Total number of clients among the cluster
  ** num_threads    -> Number of threads in each client
  ** num_coroutines -> Number of coroutines in each thread of each client
  ** current_cid    -> ID of current client
  ** genType        -> Key generation type
  ** 
  ** e.g., gen_key_multi_client(20, 2, 1, 2, 1, InputGenType::RR_SHUF) 
  **       -> Total 20 keys
  **       -> Total 2 clients
  **       -> 1 threads per client
  **       -> 2 coroutines per thread
  **       -> Currently on client 1 (second client)
  ** 
  ** Client 1 result)
  ** [Thread 0]
  **  >>> Coroutine 0
  **    Key: 11 
  **    Key: 19
  **    Key: 5
  **    Key: 17
  **    Key: 3
  ** [Thread 0]
  **  >>> Coroutine 1
  **    Key: 15 
  **    Key: 7
  **    Key: 4
  **    Key: 9
  **    Key: 13
  */

  /* Function for generating keys from scratch. */
  std::vector<K>* gen_key_multi_client(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads,
                                  uint16_t num_coroutines, uint16_t current_cid, KeyGenType genType = KeyGenType::RR_SHUF);

  /* Function for generating keys from a given dataset. */
  std::vector<K>* gen_key_multi_client(std::vector<K> keys, uint64_t num_keys, uint16_t num_clients,
                                  uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid);
};

template<typename K, typename V>
std::vector<K>* KeyGenerator<K, V>::gen_seq_keys(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid) {
  uint64_t mod = num_keys % (uint64_t)num_clients;
  uint64_t div = num_keys / (uint64_t)num_clients;

  uint64_t keys_per_client = (current_cid < mod) ? div+1 : div;

  uint64_t start = 0;
  for (uint64_t i = 0; i < current_cid; ++i) {
    if (i < mod) {
      start += div + 1;
    } else {
      start += div;
    }
  }

  // Generate keys for current client
  std::vector<K> client_keys;
  for (uint64_t i = 0; i < keys_per_client; ++i) {
    client_keys.push_back(start + i);
  }

  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines);
}

template<typename K, typename V>
std::vector<K>* KeyGenerator<K, V>::gen_rand_keys(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid) {
  std::vector<K> keys;
  for (int i = 0 ; i < num_keys; ++i) {
    keys.push_back(i);
  }
  
  std::random_device rd;
  shuffle(keys.begin(), keys.end(), std::default_random_engine(rd()));  

  uint64_t mod = num_keys % (uint64_t)num_clients;
  uint64_t div = num_keys / (uint64_t)num_clients;

  uint64_t keys_per_client = (current_cid < mod) ? div+1 : div;

  uint64_t start = 0;
  for (uint64_t i = 0; i < current_cid; ++i) {
    if (i < mod) {
      start += div + 1;
    } else {
      start += div;
    }
  }  

  // Generate keys for current client
  std::vector<K> client_keys;
  for (uint64_t i = 0; i < keys_per_client; ++i) {
    client_keys.push_back(keys[start + i]);
  }

  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines);
}

template<typename K, typename V>
std::vector<K>* KeyGenerator<K, V>::gen_rand_dup_keys(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid) {
  uint64_t mod = num_keys % (uint64_t)num_clients;
  uint64_t div = num_keys / (uint64_t)num_clients;

  uint64_t keys_per_client = (current_cid < mod) ? div+1 : div;

  std::mt19937_64 get_rand(rd());

  // Generate keys for current client
  std::vector<K> client_keys;
  for (uint64_t i = 0; i < keys_per_client; ++i) {
    client_keys.push_back((uint64_t)get_rand());
  }

  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines);
}

template<typename K, typename V>
std::vector<K>* KeyGenerator<K, V>::gen_rr_keys(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid, bool shuf) {
  // Generate keys for current client
  std::vector<K> client_keys;
  for (uint64_t i = current_cid; i < num_keys; i += num_clients) {
    client_keys.push_back(i);
  }

  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines, shuf);
}

template<typename K, typename V>
V KeyGenerator<K, V>::make_value(K key, bool random) {
  if (!random) return (V)key;
  else {
    std::mt19937_64 get_rand(rd());
    uint64_t rand = (uint64_t)get_rand();
    return (V)rand;
  }
}

template<typename K, typename V>
void KeyGenerator<K, V>::print_keys(std::vector<K>* keys, uint64_t num_threads, uint16_t num_coroutines) {
  for (uint64_t i = 0; i < num_threads; ++i) {
    fprintf(stdout, "[Thread %lu]\n", i);

    uint64_t mod = keys[i].size() % num_coroutines;
    uint64_t div = keys[i].size() / num_coroutines;

    uint64_t key_idx = 0;
    for (uint16_t j = 0; j < num_coroutines; ++j) {
      fprintf(stdout, "  >>> Coroutine %u\n", j);

      uint64_t keys_per_coroutine = (j < mod) ? div+1 : div;
      for (uint64_t k = 0; k < keys_per_coroutine; ++k) {
        fprintf(stdout, "      Key: %lu\n", keys[i][key_idx++]);
      }
    }
  }
}

template<typename K, typename V>
std::vector<K>* KeyGenerator<K, V>::gen_key_single_client(std::vector<K> keys, uint64_t num_keys, uint64_t num_threads, uint16_t num_coroutines, bool shuf) {
  if (shuf) {
    std::random_device rd;
    shuffle(keys.begin(), keys.end(), std::default_random_engine(rd()));    
  }

  uint64_t period = num_threads * (uint64_t)num_coroutines;
  uint64_t num_period = (num_keys % period) ? num_keys/period + 1 : num_keys/period;

  std::vector<K>* result = new std::vector<K>[num_threads];

  for (int i = 0; i < num_threads; ++i) {
    for (int j = 0; j < num_coroutines; ++j) {
      for (int k = 0; k < num_period; ++k) {
        if (i*num_coroutines + j + k*period < keys.size()) {
          result[i].push_back(keys[i*num_coroutines + j + k*period]);
        }
      }
    }
  }

  return result;     
}

/* Function for generating keys from scratch. */
template<typename K, typename V>
std::vector<K>* KeyGenerator<K, V>::gen_key_multi_client(uint64_t num_keys, uint16_t num_clients, uint64_t num_threads, 
                                              uint16_t num_coroutines, uint16_t current_cid, KeyGenType genType) {
  switch (genType) {
    case KeyGenType::SEQ:
      return gen_seq_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid);
    case KeyGenType::RAND:
      return gen_rand_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid);
    case KeyGenType::RAND_ALLOW_DUP:
      return gen_rand_dup_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid);
    case KeyGenType::RR:
      return gen_rr_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid, false);
    case KeyGenType::RR_SHUF:
      return gen_rr_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid, true);
    default:
      return gen_rr_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid, true);
  }
}

/* Function for generating keys from a given dataset. */
template<typename K, typename V>
std::vector<K>* KeyGenerator<K, V>::gen_key_multi_client(std::vector<K> keys, uint64_t num_keys, uint16_t num_clients,
                                              uint64_t num_threads, uint16_t num_coroutines, uint16_t current_cid) {
  // Extract keys for current client
  std::vector<K> client_keys;
  for (uint64_t i = current_cid; i < num_keys; i += num_clients) {
    client_keys.push_back(keys[i]);
  }

  // Generate keys for current client
  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines);
}