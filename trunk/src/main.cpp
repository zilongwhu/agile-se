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
#include "mempool.h"
#include "sortlist.h"
#include "idlist.h"
#include "hashtable.h"
#include "forward_index.h"

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

    bool operator <(const A &o) const
    {
        return a < o.a;
    }

    bool operator ==(const A &o) const
    {
        return a == o.a;
    }
};

int main(int argc, char *argv[])
{
//    DelayPool::init_time_updater();
//
//    DelayPool mp;
//
//    ObjectPool<int> ip;
//    ObjectPool<float> fp;
//    ObjectPool<A> ap;
//
//    ap.init(1024*1024, 20l*1024*1024*1024);
//    A *ptr = ap.alloc();
//    ap.free(ptr);
//    ptr = ap.alloc();
//    ap.delay_free(ptr);
//    ap.recycle();
//    ap.alloc<int>(1);
//    ap.alloc<int, int>(1, 2);
//    ap.alloc<int, int, int>(1, 2, 3);
//    ap.alloc<int, long, short>(1, 2, 3);
//
//    std::cout << ap.max_blocks_num() << std::endl;
//    std::cout << ap.cur_blocks_num() << std::endl;
//    std::cout << ap.block_size() << std::endl;
//    std::cout << ap.elem_size() << std::endl;
//    std::cout << ap.alloc_num() << std::endl;
//    std::cout << ap.free_num() << std::endl;
//    std::cout << ap.delayed_num() << std::endl;
//
//    std::cout << std::endl;
//    for (int i = 0; i < 6; ++i)
//    {
//        sleep(1);
//        ap.recycle();
//        std::cout << DelayPool::now() << std::endl;
//        std::cout << ap.free_num() << std::endl;
//        std::cout << ap.delayed_num() << std::endl;
//    }
//    
//    std::cout << ap.mem() << std::endl;
//
//    ObjectPool<SortList<A>::node_t> anp;
//    anp.init(4096, 1024*1024*1024);
//    SortList<A> list(&anp);
//
//    list.insert(A(5));
//    list.insert(A(3));
//    list.insert(A(1));
//    list.insert(A(2));
//    list.insert(A(4));
//
//    list.remove(A(3));
//
//    std::cout << "===" << std::endl;
//
//    SortList<A>::iterator it = list.begin();
//    while (it != list.end())
//    {
//        std::cout << it->a << std::endl;
//        ++it;
//    }
//
//    IDList ids(NULL, 0);
//
//    std::cout << "===" << std::endl;
//
//    ObjectPool<HashTable<int, int>::node_t> np;
//    np.init(4096, 4*1024*1024);
//
//    HashTable<int, int> hash1(&np, 100);
//    HashTable<int, int> hash2(&np, 1000);
//
//    hash1.set(1, 2);
//    hash1.set(2, 3);
//    hash2.set(5, 7);
//    hash2.set(3, 7);
//    hash2.set(4, 7);
//
//    int *pv;
//    hash1.get(1, &pv);
//    std::cout << *pv << std::endl;
//    hash2.get(5, &pv);
//    std::cout << *pv << std::endl;
//    hash1.remove(1);
//    std::cout << hash1.get(1) << std::endl;
//    hash2.remove(3);
//
//    std::cout << std::endl;
//    for (int i = 0; i < 6; ++i)
//    {
//        sleep(1);
//        np.recycle();
//        std::cout << DelayPool::now() << std::endl;
//        std::cout << np.free_num() << std::endl;
//        std::cout << np.delayed_num() << std::endl;
//    }

    ForwardIndex idx;
    idx.init("./conf", "fields.conf");
    return 0;
}
