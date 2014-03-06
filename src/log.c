/*
 * =====================================================================================
 *
 *       Filename:  log.c
 *
 *    Description:  log impl
 *
 *        Version:  1.0
 *        Created:  2012年05月23日 18时50分02秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Zilong.Zhu (), zilong.whu@gmail.com
 *        Company:  edu.whu
 *
 * =====================================================================================
 */

#include <unistd.h>
#include <time.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "log.h"

static const char *const s_level_name[] =
{
    "[DEBUG] ",
    "[TRACE] ",
    "[NOTICE] ",
    "[WARNING] ",
    "[FATAL] ",
};

struct log_sp
{
    char _buffer[4096];
    time_t _last_tm;
    pthread_t _tid;
    int _prefix_len;
};

static pthread_key_t g_log_key;
static int g_log_fd;

static void create_log_key()
{
    int ret = pthread_key_create(&g_log_key, free);
    if ( 0 != ret )
    {
        fprintf(stderr, "pthread_key_create error[%d].", ret);
        exit(-1);
    }
    g_log_fd = open("./lsnet.log", O_APPEND | O_WRONLY | O_CREAT, 0640);
    if (g_log_fd < 0)
    {
        fprintf(stderr, "failed to open log file[lsnet.log] error[%d].", errno);
        exit(-1);
    }
}

void err_warn(int level, const char *format, ...)
{
    static pthread_once_t s_log_once = PTHREAD_ONCE_INIT;

    if ( level > 5 || level < 0 )
    {
        return ;
    }
    pthread_once(&s_log_once, create_log_key);
    struct log_sp *st = (struct log_sp *)pthread_getspecific(g_log_key);
    if ( NULL == st )
    {
        st = (struct log_sp *)malloc(sizeof(struct log_sp));
        if ( NULL == st )
        {
            fprintf(stderr, "failed to alloc mem.");
            return ;
        }
        st->_last_tm = 0;
        st->_tid = pthread_self();
        st->_prefix_len = 0;
        int ret = pthread_setspecific(g_log_key, st);
        if ( 0 != ret )
        {
            free(st);
            fprintf(stderr, "pthread_setspecific error[%d].", ret);
            return ;
        }
    }
    time_t now = time(NULL);
    if ( now > st->_last_tm )
    {
        st->_last_tm = now;
        struct tm result;
        if (localtime_r(&now, &result))
        {
            strftime(st->_buffer, sizeof(st->_buffer), "[%Y-%m-%d %H:%M:%S] ", &result);
        }
        else
        {
            strcpy(st->_buffer, "[0000-00-00 00:00:00] "); /* strlen=22 */
        }
        st->_buffer[22] = '\0';
        snprintf(st->_buffer + 22, sizeof(st->_buffer) - 22, "[%lu]", (unsigned long)st->_tid);
        st->_prefix_len = strlen(st->_buffer);
    }
    va_list vl;

    strcpy(st->_buffer + st->_prefix_len, s_level_name[level]);
    int len = strlen(st->_buffer);

    va_start(vl, format);
    int ret = vsnprintf(st->_buffer + len, sizeof(st->_buffer) - len, format, vl);
    va_end(vl);

    if ( ret < 0 )
    {
        ret = 0;
    }
    if ( ret >= (int)sizeof(st->_buffer) - len )
    {
        ret = sizeof(st->_buffer) - len - 1;
    }
    st->_buffer[len + ret] = '\n';
    write(g_log_fd, st->_buffer, len + ret + 1);
}
