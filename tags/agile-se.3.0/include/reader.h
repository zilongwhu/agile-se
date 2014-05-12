// =====================================================================================
//
//       Filename:  reader.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/04/2014 01:06:37 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_READER_H__
#define __AGILE_SE_READER_H__

#include	<stdio.h>
#include	<stdint.h>
#include	<linux/limits.h>
#include	<string>
#include	<vector>

void split(const std::string& s, const std::string& delim, std::vector<std::string> &elems);
int readline(FILE *fp, char *line, int num, bool ignoreRest);

#ifndef P_MYLOG
#define P_MYLOG( _fmt_, args... ) \
    do{\
        ::snprintf((log_buffer), (log_buffer_length), "[%s][%s][%d] " _fmt_, __FILE__, __FUNCTION__, __LINE__, ##args);\
        reader.log(log_buffer);\
    }while(0)
#endif

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
            _max_line_no = 0;
            _file_no = 0;
            _line_no = 0;
            _eventid = 0;
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
    public:
        char _line_buf[1024*1024*10];       // 10M
    private:
        char _filepath[PATH_MAX];
        uint16_t _path_len;

        int32_t _max_line_no;

        int32_t _file_no;
        int32_t _line_no;
        int64_t _eventid;

        int32_t _partition_cur;
        int32_t _partition_num;
        std::string _inc_name;

        FILE *_fp;

        bool _snapshot;
        int32_t _ss_file_no;
        int32_t _ss_line_no;
        off_t _ss_pos;
};

#endif
