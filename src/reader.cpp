// =====================================================================================
//
//       Filename:  reader.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/04/2014 01:08:10 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fstream>
#include "log.h"
#include "reader.h"
#include "fast_timer.h"
#include "configure.h"

void split(const std::string& s, const std::string& delim, std::vector<std::string> &elems)
{
    elems.clear();

    size_t pos = 0;
    size_t len = s.length();
    size_t delim_len = delim.length();
    if (delim_len == 0) return;
    while (pos < len)
    {
        std::string::size_type find_pos = s.find(delim, pos);
        if (find_pos == std::string::npos)
        {
            elems.push_back(s.substr(pos, len - pos));
            return;
        }
        elems.push_back(s.substr(pos, find_pos - pos));
        pos = find_pos + delim_len;
    }
    elems.push_back(std::string());
    return;
}

int readline(FILE *fp, char *line, int num, bool ignoreRest)
{
    if (NULL == fp || NULL == line || num <= 0)
    {
        return -1;
    }
    off_t pos = ::ftello(fp);
    if (-1 == pos)
    {
        WARNING("ftello error[%d].", errno);
        return -1;
    }
    if (::fgets(line, num, fp) == NULL)
    { /* no data is read */
        return 0;
    }
    int len = ::strlen(line);
    if ('\n' == line[len - 1])
    { /* read a line */
        return len;
    }
    /* part of line is read */
    int cnt = 0;
    int ch;
    while (1)
    {
        ch = ::fgetc(fp);
        ++cnt;
        if (EOF == ch)/* end of file */
        {
            /* uncomplete line, undo read */
            cnt = -len; /* play a trick, should return 0 */
            break;
        }
        else if ('\n' == ch)/* end of line */
        {
            if (ignoreRest)
            {
                return len + cnt; /* tell caller real line size */
            }
            /* buffer is not large enough, undo read and tell caller */
            break;
        }
        else if (ch < 0 || ch > 0xFF)/* error */
        {
            WARNING("fgetc return %d, error[%d].", ch, errno);
            return -1;
        }
    }
    if (::fseeko(fp, pos, SEEK_SET) < 0)
    {
        WARNING("fseeko error[%d].", errno);
        return -1;
    }
    /* uncomplete line is read, undo */
    return len + cnt;
}

int IncReader::init(const char *path, const char *meta)
{
    std::string fp;
    {
        Config conf(path, meta);
        if (conf.parse() < 0)
        {
            WARNING("failed to load [%s][%s]", path, meta);
            return -1;
        }
        if (!conf.get("inc_file_prefix", fp))
        {
            WARNING("failed to get inc_file_prefix");
            return -1;
        }
        if (!conf.get("file_no", _file_no))
        {
            WARNING("failed to get file_no");
            return -1;
        }
        if (!conf.get("line_no", _line_no))
        {
            WARNING("failed to get line_no");
            return -1;
        }
        if (!conf.get("max_line_no", _max_line_no))
        {
            WARNING("failed to get max_line_no");
            return -1;
        }
        if (!conf.get("partition_cur", _partition_cur))
        {
            WARNING("failed to get partition_cur");
            return -1;
        }
        if (!conf.get("partition_num", _partition_num))
        {
            WARNING("failed to get partition_num");
            return -1;
        }
        if (!conf.get("inc_name", _inc_name))
        {
            WARNING("failed to get inc_name");
            return -1;
        }

        if (_partition_cur >= _partition_num || _partition_num == 0)
        {
            WARNING("invalid partition info[%d:%d]", _partition_cur, _partition_num);
            return -1;
        }
    }
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
        FATAL("failed to open file[%s].", _filepath);
        return -1;
    }
    if (_line_no > 0)
    {
        int ret;
        for (int32_t i = 0; i < _line_no; ++i)
        {
            ret = readline(_fp, _line_buf, sizeof _line_buf, true);
            if (ret <= 0)
            {
                if (ret < 0)
                {
                    FATAL("read line[%d] from file[%s] error.", i + 1, _filepath);
                }
                else
                {
                    FATAL("only read %d lines from file[%s], which should have %d lines.", i, _filepath, _line_no);
                }
                goto ERROR;
            }
            else if (ret >= (int)sizeof _line_buf)
            {
                WARNING("line[%d] too long in file[%s].", i + 1, _filepath);
            }
        }
        char *end = ::strchr(_line_buf, '\t');
        if (end)
        {
            *end = '\0';
        }
    }
    WARNING("init inc_reader ok, filepath[%s] fileno[%d] lineno[%d] max_lineno[%d] partition[%d:%d].",
            fp.c_str(), _file_no, _line_no, _max_line_no, _partition_cur, _partition_num);
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
        FATAL("_fp is NULL");
        return -1;
    }
    if (_line_no >= _max_line_no)
    {
        ::sprintf(_filepath + _path_len, ".%d", _file_no + 1);
        FILE *fp = ::fopen(_filepath, "r");

        if (NULL == fp)
        {
            TRACE("failed to open file[%s].", _filepath);
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
        FATAL("error occured when reading file[%s], line_no[%d].", _filepath, _line_no);
        return -1;
    }
    else if (0 == ret)
    {
        return 0; /* try again at some later time */
    }
    else if (ret >= (int)sizeof _line_buf)
    {
        WARNING("too long line at file[%s], line_no[%d], length = %d.", _filepath, _line_no, ret);
    }
    ++_line_no;
    _eventid = ::strtoul(_line_buf, NULL, 10);
    return 1;
}

int IncReader::snapshot()
{
    _ss_file_no = _file_no;
    _ss_line_no = _line_no;
    if (NULL == _fp)
    {
        WARNING("_fp is NULL, failed to make snapshot.");
        return -1;
    }
    _ss_pos = ::ftello(_fp);
    if (-1 == _ss_pos)
    {
        WARNING("ftello return -1, errno[%d].", errno);
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
                WARNING("failed to seek file[%s], line no[%d]", _filepath, _ss_line_no);
                return -1;
            }
        }
        else
        {
            ::sprintf(_filepath + _path_len, ".%d", _ss_file_no);
            FILE *fp = ::fopen(_filepath, "r");

            if (NULL == fp)
            {
                WARNING("failed to open file[%s].", _filepath);
                ::sprintf(_filepath + _path_len, ".%d", _file_no);
                return -1;
            }
            if (::fseeko(fp, _ss_pos, SEEK_SET) < 0)
            {
                WARNING("failed to seek file[%s], line no[%d]", _filepath, _ss_line_no);
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
                DEBUG("wait too long, collect only %d lines.", cnt);
                break;
            }
            ::usleep(500*1000); /* 500ms */
            continue;
        }
        else
        {
            FATAL("failed to read a complete line.");
            break;
        }
    }
    timer.stop();
    if (cnt > 0)
    {
        WARNING("collect %d lines in %d ms, start to process.", cnt, (int)timer.timeInMs());
    }
    return cnt;
}

void IncReader::log(const char *str)
{
    if (str)
    {
        WARNING("eventid[%lu] file_no[%d] line_no[%d] type[%s]: %s", _eventid, _file_no, _line_no, _inc_name.c_str(), str);
    }
}
