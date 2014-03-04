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

#include <iostream>
#include "pool/mempool.h"

struct A
{
    int a;
    int b;
    int c;

    A()
    {
        a = b = c = 0;
    }
    A(int aa)
    {
        a = aa;
        b = c = 0;
    }
    A(int aa, int bb)
    {
        a = aa;
        b = bb;
        c = 0;
    }
    A(int aa, int bb, int cc)
    {
        a = aa;
        b = bb;
        c = cc;
    }
};

int main(int argc, char *argv[])
{
    DelayPool::init_time_updater();

    DelayPool mp;

    ObjectPool<int> ip;
    ObjectPool<float> fp;
    ObjectPool<A> ap;

    ap.init(1024*1024, 20l*1024*1024*1024);
    A *ptr = ap.alloc();
    ap.free(ptr);
    ptr = ap.alloc();
    ap.delay_free(ptr);
    ap.recycle();
    ap.alloc<int>(1);
    ap.alloc<int, int>(1, 2);
    ap.alloc<int, int, int>(1, 2, 3);
    ap.alloc<int, long, short>(1, 2, 3);

    std::cout << ap.max_blocks_num() << std::endl;
    std::cout << ap.cur_blocks_num() << std::endl;
    std::cout << ap.block_size() << std::endl;
    std::cout << ap.elem_size() << std::endl;
    std::cout << ap.alloc_num() << std::endl;
    std::cout << ap.free_num() << std::endl;
    std::cout << ap.delayed_num() << std::endl;

    std::cout << std::endl;
    for (int i = 0; i < 6; ++i)
    {
        sleep(1);
        ap.recycle();
        std::cout << DelayPool::now() << std::endl;
        std::cout << ap.free_num() << std::endl;
        std::cout << ap.delayed_num() << std::endl;
    }
    
    std::cout << ap.mem() << std::endl;
    return 0;
}
