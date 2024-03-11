#include "stdafx.h"
#include "gettime.h"

#ifdef _MSC_VER
int gettime(struct timeval* tv)
{
    FILETIME ft;
    unsigned __int64 tmpres = 0;
    static int tzflag = 0;

    if (NULL != tv)
    {
        GetSystemTimeAsFileTime(&ft);

        tmpres |= ft.dwHighDateTime;
        tmpres <<= 32;
        tmpres |= ft.dwLowDateTime;

        tmpres /= 10;  /*convert into microseconds*/
        /*converting file time to unix epoch*/
        tmpres -= DELTA_EPOCH_IN_MICROSECS;
        tv->tv_sec = (long)(tmpres / 1000000UL);
        tv->tv_usec = (long)(tmpres % 1000000UL);
    }

    return 0;
}
#endif

int64_t now() {
    struct timeval tv;
#ifndef _MSC_VER
    gettimeofday(&tv, nullptr);
#else
    gettime(&tv);
#endif

    return ((int64_t)tv.tv_sec * 1000) + (tv.tv_usec / 1000);
}
