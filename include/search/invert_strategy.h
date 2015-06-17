#ifndef __AGILE_SE_INVERT_STRATEGY_H__
#define __AGILE_SE_INVERT_STRATEGY_H__

#include <stdint.h>
#include <vector>

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
            int32_t trig_bits;                  /* 触发类型标记 */
            uint32_t sign;                      /* 倒排签名 */
            data_t data;
        };

        struct doc_info_t
        {
            int32_t docid;
            int32_t outer_id;
            void *info;
        };

        virtual ~InvertStrategy() {}
        /* 倒排拉链对应回调, result是inout参数 */
        virtual void work(info_t * /* info */) = 0;
        virtual void and_work(
                const std::vector<const info_t *> & /* tokens */,
                info_t * /* result */) = 0;
        virtual void or_work(
                const std::vector<const info_t *> & /* tokens */,
                info_t * /* result */) = 0;
        /* 二元操作符拉链对应回调, result是out参数 */
        virtual void work(const info_t *left, const info_t *right,
                int list_op, info_t *result)
        { /* 默认做个转调处理 */
            std::vector<const info_t *> infos;
            if (left)
            {
                infos.push_back(left);
            }
            if (right)
            {
                infos.push_back(right);
            }
            if (LIST_OP_AND == list_op)
            {
                this->and_work(infos, result);
            }
            else
            { /* LIST_OP_DIFF不会调用该work版本函数  */
                this->or_work(infos, result);
            }
        }

        /* 新接口 */
        virtual float weight(const info_t * /* info */,
                const doc_info_t &/* doc_info */, void * /* inner result */) = 0;
};

class DummyStrategy: public InvertStrategy
{
    public:
        void work(info_t * /* info */) { }
        void and_work(const std::vector<const info_t *> & /* tokens */, info_t * /* result */) { }
        void or_work(const std::vector<const info_t *> & /* tokens */, info_t * /* result */) { }
        float weight(const info_t * /* info */, const doc_info_t &/* doc_info */,
                void * /* inner result */) { return 0.0; }
};

#endif
