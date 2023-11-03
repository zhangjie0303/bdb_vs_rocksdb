// API: https://web.deu.edu.tr/doc/berkeley/berkeleyDB/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>

#include <iostream>
//#include <errno.h>

// only this head should include for use bdb.
#include "db_cxx.h"
#include "time_helper.h"

//#define DATABASE
//"/root/zhangjie/b_plus_tree/bdb_vs_rocksdb/bdb_example/data/zhangjie.db"

const int KV_MAX_LEN = 64;
const uint64_t max_write_count = 268435456;
// const uint64_t max_write_count = 100000;
const uint64_t random_read_count = 10000;
const uint32_t stat_count_interval = 10000;
const char *bdb_data_path = "data/zhangjie.db";

// Database file name
const char *fileName = "mydb.db";

// Open a Berkeley DB database
int openDb(Db **dbpp, const char *progname, const char *fileName, DbEnv *envp,
	   u_int32_t extraFlags) {
    int ret;
    u_int32_t openFlags;

    try {
		Db *dbp = new Db(envp, 0);

		// Point to the new'd Db
		*dbpp = dbp;

		if (extraFlags != 0) ret = dbp->set_flags(extraFlags);

		// Now open the database */
		openFlags = DB_CREATE |		   // Allow database creation
				DB_READ_UNCOMMITTED |  // Allow uncommitted reads
				DB_AUTO_COMMIT;	   // Allow autocommit

		dbp->open(NULL,	      // Txn pointer
			  fileName,   // File name
			  NULL,	      // Logical db name
			  DB_BTREE,   // Database type (using btree)
			  openFlags,  // Open flags
			  0);	      // File mode. Using defaults
    } catch (DbException &e) {
		std::cerr << progname << ": openDb: db open failed:" << std::endl;
		std::cerr << e.what() << std::endl;
		return (EXIT_FAILURE);
    }

    return (EXIT_SUCCESS);
}

uint64_t countRecords(Db *dbp, DbTxn *txn) {
    Dbc *cursorp = NULL;
    uint64_t count = 0;

    try {
		// Get the cursor
		// dbp->cursor(txn, &cursorp, DB_READ_UNCOMMITTED);
		dbp->cursor(txn, &cursorp, 0);

		Dbt key, value;
		while (cursorp->get(&key, &value, DB_NEXT) == 0) {
			count++;
		}
    } catch (DbDeadlockException &de) {
		std::cerr << "countRecords: got deadlock" << std::endl;
		cursorp->close();
		throw de;
    } catch (DbException &e) {
		std::cerr << "countRecords error:" << std::endl;
		std::cerr << e.what() << std::endl;
    }

    if (cursorp != NULL) {
		try {
			cursorp->close();
		} catch (DbException &e) {
			std::cerr << "countRecords: cursor close failed:" << std::endl;
			std::cerr << e.what() << std::endl;
		}
    }

    return (count);
}

int main() {
    int ret = 0;
    int max_retries = 20;  // Max retry on a deadlock
#if 1
    // Create the database object.
	//DbEnv *envp = new DbEnv(0);
	//envp->set_flags(DB_DIRECT_DB);

    Db *dbp = new Db(0, 0);

    dbp->set_error_stream(&std::cerr);
    dbp->set_errpfx("AccessExample");
    dbp->set_pagesize(1024); /* Page size: 1K. */
    dbp->set_cachesize(0, 32 * 1024, 0);
    uint64_t start_0 = TimeHelper::micros();
    dbp->open(NULL, bdb_data_path, NULL, DB_BTREE, DB_CREATE, 0664);
    std::cout << "open cost: " << TimeHelper::micros() - start_0 << std::endl;
#else
    int ret = 0;
    int max_retries = 20;  // Max retry on a deadlock

    const char *dbHomeDir = "./txn_data";
    const char *progName = "bdb_example";
    Db *dbp = NULL;
    DbEnv *envp = NULL;
    u_int32_t envFlags;
    // Env open flags
    envFlags = DB_CREATE |     // Create the environment if it does not exist
	       DB_RECOVER |    // Run normal recovery.
	       DB_INIT_LOCK |  // Initialize the locking subsystem
	       DB_INIT_LOG |   // Initialize the logging subsystem
	       DB_INIT_TXN |   // Initialize the transactional subsystem. This
			       // also turns on logging.
	       DB_INIT_MPOOL;  // Initialize the memory pool (in-memory cache)

    try {
	// Create and open the environment
	envp = new DbEnv(0);

	// Indicate that we want db to internally perform deadlock
	// detection.  Also indicate that the transaction with
	// the fewest number of write locks will receive the
	// deadlock notification in the event of a deadlock.
	envp->set_lk_detect(DB_LOCK_MINWRITE);

	envp->open(dbHomeDir, envFlags, 0);

	// If we had utility threads (for running checkpoints or
	// deadlock detection, for example) we would spawn those
	// here. However, for a simple example such as this,
	// that is not required.

	// Open the database
	openDb(&dbp, progName, fileName, envp, DB_DUP);
	// envp, DB_DUPSORT);
    } catch (DbException &e) {
	std::cerr << "Error opening database environment: " << dbHomeDir
		  << std::endl;
	std::cerr << e.what() << std::endl;
	return (EXIT_FAILURE);
    }
#endif

    // put
    uint64_t start_a = TimeHelper::micros();
    char buf[KV_MAX_LEN];
    char rbuf[KV_MAX_LEN];
    memset(buf, 0, KV_MAX_LEN);
    memset(rbuf, 0, KV_MAX_LEN);
    for (uint64_t i = 0; i < max_write_count; i++) {
		int key_len = sprintf(buf, "abcdefghijklmn_%d_opqrstuvwxyz_key", i);
		buf[key_len] = '\0';
		int data_len = sprintf(rbuf, "berkeley_valu_%d_data_test_value", i);
		rbuf[data_len] = '\0';
		Dbt key(buf, key_len + 1);
		Dbt data(rbuf, data_len + 1);

		ret = dbp->put(0, &key, &data, DB_NOOVERWRITE);
		//if (ret == DB_KEYEXIST) {
		//	std::cout << "Key " << buf << " already exists." << std::endl;
		//}

		if (i % stat_count_interval == 0) {
			char *key_string = (char *)key.get_data();
			char *data_string = (char *)data.get_data();
			std::cout << "wirte, index: " << i << ", " << key_string << " : "
				  << data_string << "\n";
			if (ret == DB_KEYEXIST) {
				std::cout << "Key " << buf << " already exists." << std::endl;
			}
		}
    }

    uint64_t cost = TimeHelper::micros() - start_a;
    std::cout << "put success! total: " << max_write_count << std::endl;
    std::cout << "write total: " << max_write_count << ", cost: " << cost
	      << "micros, avg: " << double(cost) / max_write_count << "micros"
	      << std::endl;
    std::cout << "----------------------------------- put "
		 "done!----------------------------------------"
	      << std::endl;

    // read
    srand(8899);
    memset(buf, 0, KV_MAX_LEN);
    memset(rbuf, 0, KV_MAX_LEN);
    uint64_t start_read_micros = TimeHelper::micros();
    DbEnv *envp2 = dbp->get_env();

    for (uint64_t i = 0; i < random_read_count; i++) {
		// DbTxn *txn;
		bool retry = true;
		int retry_count = 0;
		// while (retry) {
		try {
			// envp2->txn_begin(NULL, &txn, 0);
			Dbc *dbcp;
			int key_len = sprintf(buf, "abcdefghijklmn_%d_opqrstuvwxyz_key",
					  rand() % max_write_count);
			buf[key_len] = '\0';
			Dbt key(buf, key_len + 1);
			Dbt data;
			// if ((ret = dbp->get(txn, &key, &data, 0)) != 0) {
			if ((ret = dbp->get(NULL, &key, &data, 0)) != 0) {
				dbp->err(ret, "DBcursor->get");
				throw DbException(ret);
			}

			// commit
			/*try {
				txn->commit(0);
				retry = false;
				txn = NULL;
			} catch (DbException &e) {
				std::cout << "Error on txn commit: "
					  << e.what() << std::endl;
			}*/

			if (i % stat_count_interval == 0) {
				char *key_string = (char *)key.get_data();
				char *data_string = (char *)data.get_data();
				std::cout << "read index: " << i << ", " << key_string << " : "
					  << data_string << "\n";
			}
		} catch (DbDeadlockException &) {
			// First thing that we MUST do is abort the transaction.
			// if (txn != NULL)
			//    (void)txn->abort();

			// Now we decide if we want to retry the operation.
			// If we have retried less than max_retries,
			// increment the retry count and goto retry.
			if (retry_count < max_retries) {
				std::cout << "############### Writer "
					  << ": Got DB_LOCK_DEADLOCK.\n"
					  << "Retrying write operation." << std::endl;
				retry_count++;
				retry = true;
			} else {
				// Otherwise, just give up.
				std::cerr << "Writer "
					  << ": Got DeadLockException and out of "
					  << "retries. Giving up." << std::endl;
				retry = false;
			}
		} catch (DbException &e) {
			std::cerr << "db get failed" << std::endl;
			std::cerr << e.what() << std::endl;
			// if (txn != NULL)
			//     txn->abort();
			retry = false;
		} catch (std::exception &ee) {
			std::cerr << "Unknown exception: " << ee.what() << std::endl;
			return (0);
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
#if 0
	// We put a try block around this section of code
	// to ensure that our database is properly closed
	// in the event of an error.
	//
	try {
		// Acquire a cursor for the table.
		Dbc *dbcp;
		db.cursor(NULL, &dbcp, 0);

		// Walk through the table, printing the key/data pairs.
		Dbt key;
		Dbt data;
		int index = 0;
		uint64_t start_b = get_now_ms();
		while (dbcp->get(&key, &data, DB_NEXT) == 0) {
			char *key_string = (char *)key.get_data();
			char *data_string = (char *)data.get_data();
			std::cout << "index: " << index << ", " << key_string << " : " << data_string << "\n";
			index++;
			if (index == 1000) {
				break;
			}
		}

		std::cout << "get success! index: " << index << std::endl;
		uint64_t cost_b = get_now_ms() - start_b;
        if (cost_b > 0) {
		    std::cout << "total cost: " << cost_b << "ms, avg cost: " << double(cost_b) / index << std::endl;
        } else {
		    std::cout << "total cost: " << cost_b << "ms." << std::endl;
        }
		dbcp->close();
	}
	catch (DbException &dbe) {
		std::cerr << "AccessExample: " << dbe.what() << "\n";
	}

#else

    try {
		uint64_t start_scan_micros = TimeHelper::micros();
		// Acquire a cursor for the table.
		Dbc *dbcp;
		dbp->cursor(NULL, &dbcp, 0);

		// Walk through the table, printing the key/data pairs.
		// Dbt key("foo_40000000", sizeof("foo_40000000"));
		Dbt key("abcdefghijklmn_3000_opqrstuvwxyz_key",
			sizeof("abcdefghijklmn_3000_opqrstuvwxyz_key"));
		Dbt data;

		// if ((ret = dbcp->get(&key, &data, DB_SET_RECNO)) != 0) {
		// if ((ret = dbcp->get(&key, &data, DB_CURRENT)) != 0) {
		if ((ret = dbcp->get(&key, &data, DB_SET)) != 0) {
			dbp->err(ret, "DBcursor->get");
			throw DbException(ret);
		}
		std::cout << "key: " << (char *)key.get_data()
			  << ", value: " << (char *)data.get_data()
			  << ", get cost: " << TimeHelper::micros() - start_scan_micros
			  << "micros" << std::endl;

		int index = 0;
		while (dbcp->get(&key, &data, DB_NEXT) == 0) {
			char *key_string = (char *)key.get_data();
			char *data_string = (char *)data.get_data();
			// std::cout << "index: " << index << ", " << key_string << " : " <<
			// data_string << "\n";
			index++;
			if (index == 1000) {
				break;
			}
		}

		std::cout << "scan success! scan total: " << index << std::endl;
		uint64_t cost_b = TimeHelper::micros() - start_scan_micros;
		if (cost_b > 0) {
			std::cout << "scan total cost: " << cost_b
				  << "micros, avg cost: " << double(cost_b) / index
				  << "micros" << std::endl;
		} else {
			std::cout << "scan total cost: " << cost_b << "micros."
				  << std::endl;
		}

		dbcp->close();
    } catch (DbException &dbe) {
		std::cerr << "AccessExample: " << dbe.what() << "\n";
    }
#endif
    std::cout << "----------------------------------- scan "
		 "done!----------------------------------------"
	      << std::endl;

    // count
    uint64_t start_count_micros = TimeHelper::micros();
    countRecords(dbp, NULL);
    std::cout << "count total cost: "
	      << TimeHelper::micros() - start_count_micros << "micros"
	      << std::endl;
    std::cout << "----------------------------------- count "
		 "done!----------------------------------------"
	      << std::endl;

    std::cout << "all done! ---------------" << std::endl;

    return 0;
}
