#ifndef __AGILE_SE_SEARCH_INDEX_H__
#define __AGILE_SE_SEARCH_INDEX_H__

#include	"index/index.h"

// =====================================================================================
//        Class:  SearchIndex
//  Description:  索引查询接口
// =====================================================================================
class SearchIndex
{
    public:
        SearchIndex()
        {
            m_idx = NULL;
        }
        void setIndex(Index *idx)
        {
            m_idx = idx;
        }
        /* 检查倒排类型是否合法 */
        bool is_valid_type(int8_t type) const
        {
            return m_idx->is_valid_type(type);
        }
        /* 倒排签名函数，生成倒排签名 */
        uint64_t get_sign(const char *keystr, int8_t type) const
        {
            return m_idx->get_sign(keystr, type);
        }
        /* 触发倒排拉链 */
        DocList *trigger(const char *keystr, int8_t type) const
        {
            return m_idx->trigger(keystr, type);
        }
        DocList *parse(const std::string &query, const std::vector<InvertIndex::term_t> terms) const
        {
            return m_idx->parse(query, terms);
        }
        DocList *parse_hp(const std::string &query, const std::vector<InvertIndex::term_t> &terms,
                std::string *new_query = NULL) const
        {
            return m_idx->parse_hp(query, terms, new_query);
        }
        /* 通过字段名获得字段存储偏移量 */
        int32_t get_field_offset_by_name(const char *field_name) const
        {
            return m_idx->get_field_offset_by_name(field_name);
        }
        int32_t get_field_array_offset_by_name(const char *field_name) const
        {
            return m_idx->get_field_array_offset_by_name(field_name);
        }
        /* 通过docid获取oid、正排信息 */
        void *get_info_by_docid(int32_t docid, int32_t *oid = NULL) const
        {
            return m_idx->get_info_by_docid(docid, oid);
        }
        int32_t get_id_by_oid(int32_t oid) const
        {
            return m_idx->get_id_by_oid(oid);
        }
        void *unpack_field(ForwardIndex::vaddr_t addr) const
        {
            return m_idx->unpack_field(addr);
        }
        ForwardIndex::FieldIterator get_field_iterator(void *info) const
        {
            return m_idx->get_field_iterator(info);
        }
    private:
        Index *m_idx;
};

#endif
