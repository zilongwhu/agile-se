/*
 * =====================================================================================
 *
 *       Filename:  log.h
 *
 *    Description:  log
 *
 *        Version:  1.0
 *        Created:  2012年05月23日 18时41分01秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Zilong.Zhu (), zilong.whu@gmail.com
 *        Company:  edu.whu
 *
 * =====================================================================================
 */

#ifndef __LOG_H__
#define __LOG_H__

#ifdef __cplusplus
extern "C"
{
#endif

enum
{
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_TRACE,
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_FATAL,
};

void err_warn(int level, const char *format, ...);

#define LOG_HELPER(level, format, args...) do {                           \
    err_warn(level, "[%s:%s:%d] " format,                                 \
            __FILE__, __FUNCTION__, __LINE__, ##args);                    \
} while(0)

#ifdef LOG_LEVEL

#if LOG_LEVEL<=0
#define DEBUG(format, args...)        LOG_HELPER(LOG_LEVEL_DEBUG, format, ##args)
#else
#define DEBUG(format, args...)
#endif

#if LOG_LEVEL<=1
#define TRACE(format, args...)        LOG_HELPER(LOG_LEVEL_TRACE, format, ##args)
#else
#define TRACE(format, args...)
#endif

#if LOG_LEVEL<=2
#define NOTICE(format, args...)        LOG_HELPER(LOG_LEVEL_NOTICE, format, ##args)
#else
#define NOTICE(format, args...)
#endif

#if LOG_LEVEL<=3
#define WARNING(format, args...)    LOG_HELPER(LOG_LEVEL_WARNING, format, ##args)
#else
#define WARNING(format, args...)
#endif

#else

#define DEBUG(format, args...)        LOG_HELPER(LOG_LEVEL_DEBUG, format, ##args)
#define TRACE(format, args...)        LOG_HELPER(LOG_LEVEL_TRACE, format, ##args)
#define NOTICE(format, args...)        LOG_HELPER(LOG_LEVEL_NOTICE, format, ##args)
#define WARNING(format, args...)    LOG_HELPER(LOG_LEVEL_WARNING, format, ##args)

#endif

#define FATAL(format, args...)        LOG_HELPER(LOG_LEVEL_FATAL, format, ##args)

#ifdef __cplusplus
}
#endif

#endif
