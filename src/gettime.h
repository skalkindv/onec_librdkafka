#ifndef __GETTIME__
#define __GETTIME__

#include <stdint.h>

#ifdef _MSC_VER
    #include <time.h>
    #include <windows.h>
#else
    #include <sys/time.h>
#endif

#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
#else
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000ULL
#endif

/*
struct timezone
{
    int  tz_minuteswest;  minutes W of Greenwich 
    int  tz_dsttime;      type of dst correction 
};
*/

int64_t now();

#endif