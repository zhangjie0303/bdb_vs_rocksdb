include make_config.mk

ifndef DISABLE_JEMALLOC
    ifdef JEMALLOC
        PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
    endif
    EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread -lsnappy
    PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
    CXXFLAGS += -fno-rtti
endif

LIB_GFLAGS = /usr/local/lib/libgflags.a
LIB_ROCKSDB = /opt/app/rocksdb/lib64/librocksdb.a

LIB_ALL =  ${LIB_GFLAGS} $(LIB_ROCKSDB)

INC_COMMON = -I/usr/include -I/usr/local/include
INC_ROCKSDB = -I/opt/app/rocksdb/include

INC_ALL = ${INC_COMMON} ${INC_ROCKSDB}

CFLAGS += -Wstrict-prototypes

.PHONY: clean librocksdb

all: rocks

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

rocks: rocks.cpp
	$(CXX) $(CXXFLAGS) $@.cpp -o $@ ${INC_ALL} ${LIB_ALL} -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf rocks
