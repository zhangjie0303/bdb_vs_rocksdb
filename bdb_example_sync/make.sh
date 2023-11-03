#!/bin/sh

g++ bdb_example.cpp -o bdb_example -DHAVE_CXX_STDHEADERS -I/opt/app/berkeley_db/include /opt/app/berkeley_db/lib/libdb_cxx-5.3.a -fpermissive -lpthread
