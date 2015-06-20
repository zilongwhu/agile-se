#ifndef __AGILE_SE_LEVEL_INDEX_H__
#define __AGILE_SE_LEVEL_INDEX_H__

#include <string>
#include "index/invert_index.h"
#include "index/forward_index.h"
#include "dual_dir.h"
#include "file_watcher.h"

// =====================================================================================
//        Class:  LevelIndex
//  Description:  对InvertIndex和ForwardIndex进行封装，提供统一的查询/更新接口
//                提供0/1目录dump功能
// =====================================================================================
class LevelIndex
{
    public:
        LevelIndex()
        {
            m_has_invert = false;
            m_last_merge_time = 0;
        }
        ~LevelIndex() { }

        int init(const char *path, const char *file); /* 初始化函数，调用一次 */
        std::string name() const { return m_conf.index_name; }
        size_t doc_num() const
        {
            if (m_has_invert) {
                return m_invert.doc_num();
            } else {
                return m_forward.doc_num();
            }
        }
        void recycle()
        {
            if (m_has_invert)
            {
                m_invert.recycle();
            }
            m_forward.recycle();
        }
    public:
        /* 检查倒排类型是否合法 */
        bool is_valid_type(uint8_t type) const
        {
            if (m_has_invert) {
                return m_invert.is_valid_type(type);
            } else {
                return false;
            }
        }
        /* 倒排签名函数，生成倒排签名 */
        uint64_t get_sign(const char *keystr, uint8_t type) const
        {
            if (m_has_invert) {
                return m_invert.get_sign(keystr, type);
            } else {
                return ~(uint64_t)0;
            }
        }
    public: /* 倒排触发接口 */
        DocList *trigger(const char *keystr, int8_t type) const
        {
            if (m_has_invert) {
                return m_invert.trigger(keystr, type);
            } else {
                return NULL;
            }
        }
        DocList *parse(const std::string &query,
                const std::vector<InvertIndex::term_t> &terms) const
        {
            if (m_has_invert) {
                return m_invert.parse(query, terms);
            } else {
                return NULL;
            }
        }
        DocList *parse_hp(const std::string &query,
                const std::vector<InvertIndex::term_t> &terms,
                std::string *new_query = NULL) const
        {
            if (m_has_invert) {
                return m_invert.parse_hp(query, terms, new_query);
            } else {
                return NULL;
            }
        }
    public: /* 正排查询接口 */
        int32_t get_field_array_offset_by_name(const char *field_name) const
        {
            return m_forward.get_array_offset_by_name(field_name);
        }
        void *get_info_by_docid(int32_t docid, int32_t *oid = NULL) const
        {
            return m_forward.get_info_by_id(docid, oid);
        }
        int32_t get_id_by_oid(int32_t oid) const
        {
            return m_forward.get_id_by_oid(oid);
        }
        void *unpack_field(ForwardIndex::vaddr_t addr) const
        {
            return m_forward.unpack(addr);
        }
    public: /* 获取正排字段迭代器 */
        FieldIterator get_field_iterator(void *info) const
        {
            return this->m_forward.get_field_iterator(info);
        }
    public: /* 更新接口 */
        bool forward_update(int32_t docid, const std::vector<forward_data_t> &fields)
        {
            if (m_has_invert) {
                ForwardIndex::ids_t ids;
                return m_forward.update(docid, fields, &ids)
                    && m_invert.update_docid(ids.old_id, ids.new_id);
            } else {
                return m_forward.update(docid, fields, NULL);
            }
        }
        bool update(int32_t docid, const std::vector<forward_data_t> &fields,
                const std::vector<invert_data_t> &inverts)
        {
            if (m_has_invert) {
                ForwardIndex::ids_t ids;
                if (!m_forward.update(docid, fields, &ids)) {
                    return false;
                }
                if (!m_invert.remove(ids.old_id)) {
                    return false;
                }
                return m_invert.insert(ids.new_id, inverts);
            } else {
                return this->forward_update(docid, fields);
            }
        }
        bool remove(int32_t docid)
        {
            if (m_has_invert) {
                int32_t id = 0;
                return m_forward.remove(docid, &id) && m_invert.remove(id);
            } else {
                return m_forward.remove(docid, NULL);
            }
        }
    public:
        int dump(const char *path = NULL); /* dump索引到磁盘，0/1目录切换 */
        void print_meta() const
        {
            if (m_has_invert)
            {
                m_invert.print_meta();
            }
            m_forward.print_meta();
        }
        void print_list() const
        {
            if (m_has_invert)
            {
                m_invert.print_list_length(m_conf.print_list_file.c_str());
            }
        }
        void exc_cmd() const
        {
            if (m_has_invert)
            {
                m_invert.exc_cmd();
            }
        }
        bool try2dump() /* 若dump_flag文件更新，则dump索引 */
        {
            if (m_dump_fw.check_and_update_timestamp() > 0)
            {
                this->dump();
                return true;
            }
            return false;
        }
        void try_print_meta()
        {
            if (m_print_meta_fw.check_and_update_timestamp() > 0)
            {
                this->print_meta();
            }
        }
        void try_print_list()
        {
            if (m_print_list_fw.check_and_update_timestamp() > 0)
            {
                this->print_list();
            }
        }
        void try_exc_cmd()
        {
            if (m_has_invert)
            {
                m_invert.try_exc_cmd();
            }
        }
        void try2merge()
        {
            if (m_has_invert && g_now_time >=
                    m_last_merge_time + m_conf.merge_interval)
            {
#ifndef __NOT_USE_COWBTREE__
                P_WARNING("start to merge");
                m_invert.mergeAll(0);
                P_WARNING("end of merge");
                m_last_merge_time = g_now_time;
#endif
            }
        }
    private:
        ForwardIndex m_forward;
        InvertIndex m_invert;
        DualDir m_dual_dir; /* 0,1目录控制器 */

        bool m_has_invert;
        time_t m_last_merge_time;

        FileWatcher m_dump_fw;
        FileWatcher m_print_meta_fw;
        FileWatcher m_print_list_fw;

        struct
        {
            std::string index_name;
            std::string index_path;

            std::string forward_path;
            std::string forward_file;
            std::string invert_path;
            std::string invert_file;

            std::string dump_flag_file;
            std::string print_meta_flag_file;
            std::string print_list_flag_file;
            std::string print_list_file;

            int32_t rebuild_index;
            int32_t merge_interval;
        } m_conf;
};

#endif
