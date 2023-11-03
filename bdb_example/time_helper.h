#ifndef __TIME_HELPER_H__
#define __TIME_HELPER_H__

#include <chrono>

// NB: ALL OF THESE 3 FUNCTIONS BELOW USE SIGNED VALUES INTERNALLY AND WILL EVENTUALLY OVERFLOW (AFTER 200+ YEARS OR
// SO), AFTER WHICH POINT THEY WILL HAVE *SIGNED OVERFLOW*, WHICH IS UNDEFINED BEHAVIOR (IE: A BUG) FOR C/C++.
// But...that's ok...this "bug" is designed into the C++11 specification, so whatever. Your machine won't run for 200
// years anyway...

class TimeHelper {
public:
	// Get time stamp in milliseconds.
	static uint64_t millis()
	{
		uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::
					  now().time_since_epoch()).count();
		return ms; 
	}

	// Get time stamp in microseconds.
	static uint64_t micros()
	{
		uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::
					  now().time_since_epoch()).count();
		return us; 
	}

	// Get time stamp in nanoseconds.
	static uint64_t nanos()
	{
		uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::
					  now().time_since_epoch()).count();
		return ns; 
	}
};

#endif
