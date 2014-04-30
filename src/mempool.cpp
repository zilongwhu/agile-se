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
#include "mempool.h"
#include "multi_mempool.h"

//volatile int DelayPool::s_now;
//
//static void *pool_timer_thread(void *ptr)
//{
//    volatile int &now = *(volatile int *)ptr;
//    pthread_detach(pthread_self());
//
//    while (1)
//    {
//        now = ::time(NULL);
//        ::usleep(500*1000);
//    }
//    return NULL;
//}
//
//void DelayPool::init_time_updater()
//{
//    s_now = ::time(NULL);
//
//    pthread_t pid;
//    int ret = ::pthread_create(&pid, NULL, pool_timer_thread, (void *)&s_now);
//    if (ret != 0)
//    {
//        ::exit(-1);
//    }
//}
