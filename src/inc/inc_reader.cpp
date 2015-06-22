#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <fstream>
#include "inc/inc_reader.h"
#include "log_utils.h"
#include "str_utils.h"
#include "fast_timer.h"
#include "configure.h"

namespace inc
{
    int IncReader::init(const char *path, const char *meta)
    {
        std::string fp;
        TRY
        {
            Config conf(path, meta);
            if (conf.parse() != 0)
            {
                P_WARNING("failed to parse [%s][%s]", path, meta);
                return -1;
            }
            fp = conf["inc_file_prefix"];
            bool ret = true;
            ret = ret && parseUInt32(conf["file_no"].c_str(), _file_no);
            ret = ret && parseUInt32(conf["line_no"].c_str(), _line_no);
            ret = ret && parseUInt32(conf["max_line_no"].c_str(), _max_line_no);
            ret = ret && parseUInt32(conf["partition_cur"].c_str(), _partition_cur);
            ret = ret && parseUInt32(conf["partition_num"].c_str(), _partition_num);
            ret = ret && parseUInt32(conf["base_mode"].c_str(), _base_mode);
            //_inc_name = conf["inc_name"];

            if (!ret)
            {
                P_WARNING("failed to load parameters");
                return -1;
            }
            if (_partition_cur >= _partition_num || _partition_num == 0)
            {
                P_WARNING("invalid partition info[%u:%u]", _partition_cur, _partition_num);
                return -1;
            }
        }
        WARNING_CATCH_EXC(return -1, "failed to load conf from [%s] [%s]", path, meta);
        if (_fp)
        {
            ::fclose(_fp);
            _fp = NULL;
        }
        ::snprintf(_filepath, sizeof(_filepath) - sizeof(".xxxxxxxxxx"), "%s", fp.c_str());
        _path_len = ::strlen(_filepath);
    
        ::sprintf(_filepath + _path_len, ".%d", _file_no);
        _fp = ::fopen(_filepath, "r");
    
        if (NULL == _fp)
        {
            P_FATAL("failed to open file[%s].", _filepath);
            return -1;
        }
        if (_line_no > 0)
        {
            int ret;
            for (uint32_t i = 0; i < _line_no; ++i)
            {
                ret = readline(_fp, _line_buf, sizeof _line_buf, true);
                if (ret <= 0)
                {
                    if (ret < 0)
                    {
                        P_FATAL("read line[%u] from file[%s] error.", i + 1, _filepath);
                    }
                    else
                    {
                        P_FATAL("only read %u lines from file[%s], which should have %u lines.", i, _filepath, _line_no);
                    }
                    goto ERROR;
                }
                else if (ret >= (int)sizeof _line_buf)
                {
                    P_WARNING("line[%u] too long in file[%s].", i + 1, _filepath);
                }
            }
            char *end = ::strchr(_line_buf, '\t');
            if (end)
            {
                *end = '\0';
            }
        }
        P_WARNING("init inc_reader ok, filepath[%s] fileno[%u] lineno[%u] max_lineno[%u] partition[%u:%u]%s.",
                fp.c_str(), _file_no, _line_no, _max_line_no, _partition_cur, _partition_num,
                _base_mode ? " base" : "");
        return 0;
ERROR:
        ::fclose(_fp);
        _fp = NULL;
        return -1;
    }

    int IncReader::dumpMeta(const char *path, const char *meta)
    {
        std::string p(path);
        p += "/" + std::string(meta);
        std::ofstream fout(p.c_str());
        fout << "inc_file_prefix: " << std::string(_filepath, _path_len) << std::endl;
        fout << "file_no: " << _file_no << std::endl;
        fout << "line_no: " << _line_no << std::endl;
        fout << "max_line_no: " << _max_line_no << std::endl;
        fout << "partition_cur: " << _partition_cur << std::endl;
        fout << "partition_num: " << _partition_num << std::endl;
        fout.close();
        return fout ? 0 : -1;
    }
    
    int IncReader::next()
    {
        if (NULL == _fp)
        {
            P_FATAL("_fp is NULL");
            return -1;
        }
        if (_line_no >= _max_line_no)
        {
            ::sprintf(_filepath + _path_len, ".%d", _file_no + 1);
            FILE *fp = ::fopen(_filepath, "r");
    
            if (NULL == fp)
            {
                P_WARNING("failed to open file[%s].", _filepath);
                return 0; /* try again at some later time */
            }
            ::fclose(_fp);
    
            _fp = fp;
            ++_file_no;
            _line_no = 0;
        }
        int ret = readline(_fp, _line_buf, sizeof _line_buf, true);
        if (ret < 0)
        {
            P_FATAL("error occured when reading file[%s], line_no[%u].", _filepath, _line_no);
            return -1;
        }
        else if (0 == ret)
        {
            return 0; /* try again at some later time */
        }
        else if (ret >= (int)sizeof _line_buf)
        {
            P_WARNING("too long line at file[%s], line_no[%u], length = %d.", _filepath, _line_no, ret);
        }
        ++_line_no;
        _eventid = 0;
        if (!_base_mode)
        {
            _eventid = ::strtoul(_line_buf, NULL, 10);
        }
        return 1;
    }

    void IncReader::split(std::vector<std::string> &columns, const std::string &sep)
    {
        columns.clear();
        ::split(_line_buf, sep, columns);
        if (_base_mode && columns.size() > 1)
        {
            columns.erase(columns.begin());
        }
    }
    
    int IncReader::snapshot()
    {
        _ss_file_no = _file_no;
        _ss_line_no = _line_no;
        if (NULL == _fp)
        {
            P_WARNING("_fp is NULL, failed to make snapshot.");
            return -1;
        }
        _ss_pos = ::ftello(_fp);
        if (-1 == _ss_pos)
        {
            P_WARNING("ftello return -1, errno[%d].", errno);
            return -1;
        }
        _snapshot = true;
        return 0;
    }
    
    int IncReader::rollback()
    {
        if (_snapshot)
        {
            if (_ss_file_no == _file_no)
            {
                if (::fseeko(_fp, _ss_pos, SEEK_SET) < 0)
                {
                    P_WARNING("failed to seek file[%s], line no[%u]", _filepath, _ss_line_no);
                    return -1;
                }
            }
            else
            {
                ::sprintf(_filepath + _path_len, ".%d", _ss_file_no);
                FILE *fp = ::fopen(_filepath, "r");
    
                if (NULL == fp)
                {
                    P_WARNING("failed to open file[%s].", _filepath);
                    ::sprintf(_filepath + _path_len, ".%d", _file_no);
                    return -1;
                }
                if (::fseeko(fp, _ss_pos, SEEK_SET) < 0)
                {
                    P_WARNING("failed to seek file[%s], line no[%u]", _filepath, _ss_line_no);
                    ::sprintf(_filepath + _path_len, ".%d", _file_no);
                    ::fclose(fp);
                    return -1;
                }
    
                ::fclose(_fp);
                _fp = fp;
            }
            _file_no = _ss_file_no;
            _line_no = _ss_line_no;
            _snapshot = false;
        }
        return 0;
    }
    
    int IncReader::collect_lines(int max_lines, int max_ms)
    {
        int ret;
        int cnt;
        FastTimer timer;
    
        timer.start();
        for (cnt = 0; cnt < max_lines; )
        {
            ret = this->next();
            if (1 == ret)
            {
                ++cnt;
                continue;
            }
            else if (0 == ret)
            {
                timer.stop();
                if (timer.timeInMs() > max_ms)
                { /* wait too long */
                    P_DEBUG("wait too long, collect only %d lines.", cnt);
                    break;
                }
                ::usleep(500*1000); /* 500ms */
                continue;
            }
            else
            {
                P_FATAL("failed to read a complete line.");
                break;
            }
        }
        timer.stop();
        if (cnt > 0)
        {
            P_WARNING("collect %d lines in %d ms, start to process.", cnt, (int)timer.timeInMs());
        }
        return cnt;
    }
    
    void IncReader::log(const char *str)
    {
        if (str)
        {
            if (_base_mode)
            {
                P_WARNING("file_no[%u] line_no[%u] type[%s]: %s", _file_no, _line_no, _inc_name.c_str(), str);
            }
            else
            {
                P_WARNING("eventid[%lu] file_no[%u] line_no[%u] type[%s]: %s", _eventid, _file_no, _line_no, _inc_name.c_str(), str);
            }
        }
    }
}
