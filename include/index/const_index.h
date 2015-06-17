#ifndef __AGILE_SE_CONST_INDEX_H__
#define __AGILE_SE_CONST_INDEX_H__

#include "inc/inc_reader.h"
#include "index/forward_index.h"
#include "index/invert_index.h"

struct base_build_t
{
    inc::IncReader reader;
    InvertIndex *invert;
    ForwardIndex *forward;
};
typedef bool (*base_func_t)(base_build_t &);

// =====================================================================================
//  Description:  双buffer版本的静态索引，相比支持增量更新的索引要简单的多
//                增量版本的诸多功能不支持（包括id映射等）
// =====================================================================================
class ConstIndex
{
    public:
        ConstIndex() { }
        ~ConstIndex() { }

        int init(const char *path, const char *file); /* 初始化函数，调用一次 */
        bool load(const char *dir);
        bool dump(const char *dir);
        bool build(const char *path, const char *file);
    public:
        /* 检查倒排类型是否合法 */
        bool is_valid_type(uint8_t type) const
        {
            return m_invert.is_valid_type(type);
        }
        /* 倒排签名函数，生成倒排签名 */
        uint64_t get_sign(const char *keystr, uint8_t type) const
        {
            return m_invert.get_sign(keystr, type);
        }
    public: /* 倒排触发接口 */
        DocList *trigger(const char *keystr, int8_t type) const
        {
            return m_invert.trigger(keystr, type);
        }
        DocList *parse(const std::string &query,
                const std::vector<InvertIndex::term_t> &terms,
                std::string *new_query = NULL) const
        { /* 默认使用新引擎 */
            return m_invert.parse_hp(query, terms, new_query);
        }
    public: /* 正排查询接口 */
        int32_t get_field_array_offset_by_name(const char *field_name) const
        {
            return m_forward.get_array_offset_by_name(field_name);
        }
        void *get_info_by_docid(int32_t docid) const
        {
            return m_forward.get_info_by_id(docid, NULL);
        }
    private:
        InvertIndex m_invert;
        ForwardIndex m_forward;

        struct
        {
            std::string invert_path;
            std::string invert_file;

            std::string forward_path;
            std::string forward_file;

            std::string base_processor;
        } m_conf;
    public:
        static std::map<std::string, base_func_t> s_base_processors;
};

#define DECLARE_BASE_PROCESSOR(processor) bool processor(base_build_t &args)

#define REGISTER_BASE_PROCESSOR(processor) \
    do\
    {\
        ConstIndex::s_base_processors[#processor] = processor;\
    } while(0)

#endif
