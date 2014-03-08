// =====================================================================================
//
//       Filename:  strategy.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/07/2014 10:11:40 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_INVERT_STRATEGY_H__
#define __AGILE_SE_INVERT_STRATEGY_H__

#include <stdint.h>

enum
{
    LIST_OP_OR = 1,                             /* A|B, 并集 */
    LIST_OP_AND,                                /* A&B，交集 */
    LIST_OP_DIFF,                               /* A-B，差集 */
};

class InvertStrategy
{
    public:
        union data_t
        {
            int32_t i32;
            int64_t i64;
            void *ptr;
        };

        struct info_t
        {
            int8_t result[512];                 /* 策略数据 */
            uint16_t length;                    /* 数据长度 */
            uint8_t type;                       /* 倒排类型 */
            uint64_t sign;                      /* 倒排签名 */
            data_t data;
        };

        virtual ~InvertStrategy() {}
        /* 倒排拉链对应回调, result是inout参数 */
        virtual void work(info_t *info) = 0;
        /* 二元操作符拉链对应回调, result是out参数 */
        virtual void work(const info_t *left, const info_t *right, int list_op, info_t *result) = 0;
};

class DummyStrategy: public InvertStrategy
{
    public:
        void work(info_t *info) { }
        void work(const info_t *left, const info_t *right, int list_op, info_t *result) { }
};

#endif
