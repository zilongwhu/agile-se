#ifndef __AGILE_SE_INDEX_H__
#define __AGILE_SE_INDEX_H__

#include <map>
#include <vector>
#include <string>
#include "index/level_index.h"
#include "inc/inc_reader.h"

void init_nbslib();
typedef void *(*thread_func_t)(void *);

class Index
{
    public:
        Index() { }
        ~Index();

        int init(const char *path, const char *file); /* 初始化函数，调用一次 */
        void recycle();

        /* dump索引到磁盘，path == NULL->0/1目录切换 */
        int dump(const char *path = NULL);
        void print_meta() const;
        void print_list() const;
        void exc_cmd() const;

        bool try2dump();
        void try_print_meta();
        void try_print_list();
        void try_exc_cmd();
        void try2merge();
    public:
        LevelIndex *get_level_index(size_t level)
        {
            if (level >= m_index.size()) {
                return NULL;
            } else {
                return m_index[level];
            }
        }
    public:
        bool is_base_mode() const
        {
            return m_inc_reader.is_base_mode();
        }
        int32_t inc_das_warning_time() const
        {
            return m_conf.inc_das_warning_time;
        }
    public:
        inc::IncReader m_inc_reader;
    private:
        std::vector<LevelIndex *> m_index;
        std::map<size_t, std::string> m_level2dirname;
        DualDir m_dual_dir; /* 0,1目录控制器 */
        FileWatcher m_dump_fw;
        struct
        {
            std::string index_path;
            std::string index_meta_file;
            std::string dump_flag_file;
            std::string inc_processor;
            int32_t inc_das_warning_time;
        } m_conf;
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
