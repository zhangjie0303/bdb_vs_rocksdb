#include <gflags/gflags.h>

#include <cstdio>
#include <iostream>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_util.h"
#include "time_helper.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::ConfigOptions;
using ROCKSDB_NAMESPACE::DBOptions;  // zhangjie
using ROCKSDB_NAMESPACE::kDefaultColumnFamilyName;

const uint64_t max_write_count = 268435456;
// const uint64_t max_write_count = 100000;
const uint64_t random_read_count = 10000;
const uint32_t stat_count_interval = 10000;
const std::string rocksdb_data_path = "data/my_data";

void get_kv(uint64_t index, std::string& key, std::string& value) {
    std::string index_str = std::to_string(index);
    key = "abcdefghijklmn_" + index_str + "_opqrstuvwxyz_key";
    value = "rocksdb_value_" + index_str + "_data_test_value";
}

void get_key(uint64_t index, std::string& key) {
    key = "abcdefghijklmn_" + std::to_string(index) + "_opqrstuvwxyz_key";
}

int main(int argc, char** argv) {
    DB* db;
    Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
	// for direct io -- zhangjie
	options.use_direct_reads = true;

    // open DB
    Status s = DB::Open(options, rocksdb_data_path, &db);
    if (!s.ok()) {
	std::cout << "open failed, status: " << s.ToString() << std::endl;
	return -1;
    }

    // put
    auto wirte_options = WriteOptions();
    uint64_t start_micros = TimeHelper::micros();
    for (uint64_t i = 0; i < max_write_count; i++) {
	std::string key;
	std::string value;
	get_kv(i, key, value);
	// Put key-value
	s = db->Put(wirte_options, key, value);
	if (s.ok()) {
	    if (i % stat_count_interval == 0) {
		std::cout << "DEBUG, put OK: " << i << ", " << key << " | "
			  << value << std::endl;
	    }
	} else {
	    std::cout << "ERROR, put not OK: " << key << " | " << value
		      << " | start_id: 0"
			 ", current count: "
		      << i << ", wirte_cout: " << max_write_count << std::endl;
	    // break;
	}
    }
    uint64_t cost_micros = TimeHelper::micros() - start_micros;
    std::cout << "wirte total: " << max_write_count << ", cost: " << cost_micros
	      << "micros, avg: " << double(cost_micros) / max_write_count
	      << "micros" << std::endl;
    std::cout << "----------------------------------- put "
		 "done!----------------------------------------"
	      << std::endl;

    // read
    auto read_options = ReadOptions();
    srand(8899);
    uint64_t start_read_micros = TimeHelper::micros();
    for (uint64_t i = 0; i < random_read_count; i++) {
	std::string key;
	std::string value;
	get_key(rand() % max_write_count, key);
	s = db->Get(read_options, key, &value);
	if (s.ok()) {
	    if (i % stat_count_interval == 0) {
		std::cout << "DEBUG, get OK: " << i << ", " << key << " | "
			  << value << std::endl;
	    }
	} else {
	    std::cout << "ERROR, get not OK: " << key << " | " << value
		      << std::endl;
	}
    }
    uint64_t cost_read_micros = TimeHelper::micros() - start_read_micros;
    std::cout << "read total: " << random_read_count
	      << ", cost: " << cost_read_micros
	      << "micros, avg: " << double(cost_read_micros) / random_read_count
	      << "micros" << std::endl;

    std::cout << "----------------------------------- read "
		 "done!----------------------------------------"
	      << std::endl;
    // scan
    int index = 0;
    uint64_t start_scan_micros = TimeHelper::micros();
    Iterator* it = db->NewIterator(ReadOptions());
    for (it->Seek("abcdefghijklmn_3000_opqrstuvwxyz_key"); it->Valid();
	 it->Next()) {
	index++;
	// std::cout << "index: " << index << ", key: " << it->key().ToString()
	// << " value: " << it->value().ToString() << std::endl;
	if (index == 1000) {
	    break;
	}
    }

    std::cout << "scan success! scan total: " << index << std::endl;
    uint64_t cost_scan = TimeHelper::micros() - start_scan_micros;
    std::cout << "scan total cost: " << cost_scan
	      << "micros, avg cost: " << double(cost_scan) / index << "micros"
	      << std::endl;

    std::cout << "----------------------------------- scan "
		 "done!----------------------------------------"
	      << std::endl;
    // count
    uint64_t start_count_micros = TimeHelper::micros();
    uint64_t count = 0;
    it = db->NewIterator(ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
	count++;
    }
    std::cout << "count success! count total: " << count << std::endl;
    uint64_t cost_count = TimeHelper::micros() - start_count_micros;
    std::cout << "count total cost: " << cost_count << "micros" << std::endl;
    std::cout << "----------------------------------- count "
		 "done!----------------------------------------"
	      << std::endl;

    return 0;
}
