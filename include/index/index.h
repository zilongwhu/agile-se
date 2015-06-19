#ifndef __AGILE_SE_INDEX_H__
#define __AGILE_SE_INDEX_H__

#include	<pthread.h>
#include	<string>
#include	"index/invert_index.h"
#include	"index/forward_index.h"
#include	"dual_dir.h"
#include	"file_watcher.h"
#include	"inc/inc_reader.h"

void init_nbslib();
typedef void *(*thread_func_t)(void *);

// =====================================================================================
//        Class:  Index
//  Description:  对InvertIndex和ForwardIndex进行封装，提供统一的查询/更新接口
//                提供0/1目录dump功能
// =====================================================================================
class Index
{
    public:
        Index()
        {
            m_last_merge_time = 0;
        }
        ~Index() { }

        int init(const char *path, const char *file); /* 初始化函数，调用一次 */

        void recycle()
        {
            m_invert.recycle();
            m_forward.recycle();
        }

        int dump(); /* dump索引到磁盘，0/1目录切换 */
        bool try2dump() /* 若dump_flag文件更新，则dump索引 */
        {
            if (m_dump_fw.check_and_update_timestamp() > 0)
            {
                this->dump();
                return true;
            }
            return false;
        }
        void print_meta() const
        {
            m_invert.print_meta();
            m_forward.print_meta();
        }
        void try_print_meta()
        {
            if (m_print_meta_fw.check_and_update_timestamp() > 0)
            {
                this->print_meta();
            }
        }
        void print_list() const { m_invert.print_list_length(m_conf.print_list_file.c_str()); }
        void try_print_list()
        {
            if (m_print_list_fw.check_and_update_timestamp() > 0)
            {
                this->print_list();
            }
        }
        void exc_cmd() const { m_invert.exc_cmd(); }
        void try_exc_cmd() { m_invert.try_exc_cmd(); }

        size_t docs_num() const { return m_invert.docs_num(); }
        bool is_base_mode() const { return m_conf.base_mode != 0; }

        void try2merge()
        {
            if (g_now_time >= m_last_merge_time + m_conf.merge_interval)
            {
#ifndef __NOT_USE_COWBTREE__
                P_WARNING("start to merge");
                m_invert.mergeAll(0);
                P_WARNING("end of merge");
                m_last_merge_time = g_now_time;
#endif
            }
        }
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
                const std::vector<InvertIndex::term_t> &terms) const
        {
            return m_invert.parse(query, terms);
        }
        DocList *parse_hp(const std::string &query,
                const std::vector<InvertIndex::term_t> &terms,
                std::string *new_query = NULL) const
        {
            return m_invert.parse_hp(query, terms, new_query);
        }
    public: /* 正排查询接口 */
        int32_t get_field_offset_by_name(const char *field_name) const
        {
            return m_forward.get_offset_by_name(field_name);
        }
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
        ForwardIndex::FieldIterator get_field_iterator(void *info) const
        {
            return this->m_forward.get_field_iterator(info);
        }
    public: /* 更新接口 */
        /* 正排更新 */
        bool forward_update(int32_t docid, const std::vector<forward_data_t> &fields,
                ForwardIndex::ids_t *p_ids = NULL)
        {
            return m_forward.update(docid, fields, p_ids);
        }
        bool forward_remove(int32_t docid, int32_t *p_id = NULL)
        {
            return m_forward.remove(docid, p_id);
        }
        /* 倒排更新 */
        bool invert_insert(const char *keystr, uint8_t type, int32_t docid, const std::string &json)
        {
            return m_invert.insert(keystr, type, docid, json);
        }
        bool invert_insert(const char *keystr, uint8_t type, int32_t docid, cJSON *json)
        {
            return m_invert.insert(keystr, type, docid, json);
        }
        bool invert_remove(const char *keystr, int8_t type, int32_t docid)
        {
            return m_invert.remove(keystr, type, docid);
        }
        bool invert_remove(int32_t docid)
        {
            return m_invert.remove(docid);
        }
        bool update_docid(int32_t from, int32_t to)
        {
            return m_invert.update_docid(from, to);
        }
    public:
        int32_t inc_das_warning_time() const
        {
            return m_conf.inc_das_warning_time;
        }

        inc::IncReader m_inc_reader;
    private:
        InvertIndex m_invert;
        ForwardIndex m_forward;
        DualDir m_dual_dir; /* 0,1目录控制器 */

        struct
        {
            std::string invert_path;
            std::string invert_file;

            std::string forward_path;
            std::string forward_file;

            std::string dump_flag_file;
            std::string print_meta_flag_file;
            std::string print_list_flag_file;
            std::string print_list_file;

            std::string index_path;
            std::string index_meta_file;

            std::string inc_processor;

            int32_t inc_das_warning_time;

            int32_t rebuild_index;
            int32_t base_mode;
            int32_t merge_interval;
        } m_conf;
        FileWatcher m_dump_fw;
        FileWatcher m_print_meta_fw;
        FileWatcher m_print_list_fw;

        time_t m_last_merge_time;

        pthread_t m_inc_tid;
    public:
        static std::map<std::string, thread_func_t> s_inc_processors;
};

#define DECLARE_INC_PROCESSOR(processor) void *processor(void *)

#define REGISTER_INC_PROCESSOR(processor) \
    do\
    {\
        Index::s_inc_processors[#processor] = processor;\
    } while(0)

#endif
