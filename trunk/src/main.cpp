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

int main(int argc, char *argv[])
{
    DelayPool::init_time_updater();

    DelayPool mp;
    return 0;
}
