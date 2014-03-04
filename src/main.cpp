// =====================================================================================
//
//       Filename:  main.cpp
//
//    Description:  main file
//
//        Version:  1.0
//        Created:  03/03/2014 02:07:51 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include "pool/mempool.h"

struct A
{
    int a;
    int b;
    int c;
};

int main(int argc, char *argv[])
{
    DelayPool::init_time_updater();

    DelayPool mp;

    ObjectPool<int> ip;
    ObjectPool<float> fp;
    ObjectPool<A> ap;

    ap.init(4096, 1024*1024*1024);
    A *ptr = ap.alloc();
    ap.free(ptr);
    ptr = ap.alloc();
    ap.delay_free(ptr);
    ap.recycle();
    return 0;
}
