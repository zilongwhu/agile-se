#ifndef __AGILE_SE_INC_READER_H__
#define __AGILE_SE_INC_READER_H__

#include <stdio.h>
#include <stdint.h>
#include <string>
#include <vector>

#ifndef P_MYLOG
#define P_MYLOG( _fmt_, args... ) \
	do{\
	    ::snprintf( log_buffer, log_buffer_length, "[%s][%s][%d] " _fmt_, __FILE__, __FUNCTION__, __LINE__, ##args );\
        reader.log(log_buffer);\
	}while(0)
#endif

#ifndef PATH_MAX
#define PATH_MAX (1024)
#endif

namespace inc
{
    class IncReader
    {
        private:
            IncReader(const IncReader &);
            IncReader &operator =(const IncReader &);
        public:
            IncReader()
                : _fp(NULL)
            {
                _filepath[0] = '\0';
                _path_len = 0;
                _snapshot = false;
            }
            ~IncReader()
            {
                if (_fp)
                {
                    ::fclose(_fp);
                    _fp = NULL;
                }
            }
    
            int init(const char *path, const char *meta);
            int dumpMeta(const char *path, const char *meta);
    
            int snapshot();
            int rollback();
    
            int next();
            int collect_lines(int max_lines, int max_ms);
    
            void log(const char *str);
    
            uint32_t current_partition() const { return _partition_cur; }
            uint32_t total_partition() const { return _partition_num; }
            const char* get_filepath() const { return _filepath; }
            uint32_t get_file_no() const { return _file_no; }
            uint32_t get_line_no() const { return _line_no; }
            uint32_t get_eventid() const { return _eventid; }
        public:
            char _line_buf[1024*1024*10];       // 10M
        private:
            char _filepath[PATH_MAX];
            uint16_t _path_len;
    
            uint32_t _max_line_no;
    
            uint32_t _file_no;
            uint32_t _line_no;
            uint64_t _eventid;
    
            uint32_t _partition_cur;
            uint32_t _partition_num;
            uint32_t _base_mode;
            std::string _inc_name;
    
            FILE *_fp;

            bool _snapshot;
            uint32_t _ss_file_no;
            uint32_t _ss_line_no;
            off_t _ss_pos;
    };
}

#endif
