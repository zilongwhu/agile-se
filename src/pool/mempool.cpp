// =====================================================================================
//
//       Filename:  mempool.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/03/2014 05:52:32 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include "pool/mempool.h"

int DelayPool::s_now;

static void *pool_timer_thread(void *ptr)
{
    int &now = *(int *)ptr;
    pthread_detach(pthread_self());

    while (1)
    {
        now = ::time(NULL);
        ::usleep(500*1000);
    }
    return NULL;
}

void DelayPool::init_time_updater()
{
    pthread_t pid;
    int ret = ::pthread_create(&pid, NULL, pool_timer_thread, &s_now);
    if (ret != 0)
    {
        ::exit(-1);
    }
}
