#!/bin/sh

g++ bdb_example.cpp -o bdb_example -DHAVE_CXX_STDHEADERS -I/opt/app/berkeley_db_direct_io/include /opt/app/berkeley_db_direct_io/lib/libdb_cxx-5.3.a -fpermissive -lpthread
