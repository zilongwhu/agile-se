#ifndef __AGILE_SE_FILE_WATCHER_H__
#define __AGILE_SE_FILE_WATCHER_H__

#include <sys/time.h>
#include <sys/stat.h>
#include <string>
#include "utils/log.h"

class FileWatcher
{
    public:
        static const time_t NON_EXIST_TS = (time_t)-1;
        static const int SKEW = -2;
        static const int DELETED = -1;
        static const int UNCHANGED = 0;
        static const int UPDATED = 1; 
        static const int CREATED = 2;
        
        explicit FileWatcher ()
            : checked_ts_(NON_EXIST_TS) , last_ts_(NON_EXIST_TS)
        {}
    
        int create (const char* file_path)
        {
            if (NULL == file_path) {
                P_FATAL("file_path is NULL");
                return -1;
            }
            
            file_path_ = file_path;
    
            struct stat tmp_st;
            int ret = ::stat(file_path_.c_str(), &tmp_st);
            if (ret < 0) {
                P_WARNING("%s does not exist", file_path_.c_str());
                checked_ts_ = NON_EXIST_TS;
                last_ts_ = NON_EXIST_TS;
            } else {
                checked_ts_ = tmp_st.st_mtime;
                last_ts_ = tmp_st.st_mtime;
            }
            return 0;
        }
    
        int is_timestamp_updated ()
        {
            struct stat tmp_st;
            const int ret = ::stat(file_path_.c_str(), &tmp_st);
            if (ret < 0) {
                checked_ts_ = NON_EXIST_TS;
                if (NON_EXIST_TS != last_ts_) {
                    return DELETED;
                } else {
                    return UNCHANGED;
                }
            } else {
                checked_ts_ = tmp_st.st_mtime;
                if (NON_EXIST_TS != last_ts_) {
                    if (checked_ts_ > last_ts_) {
                        return UPDATED;
                    } else if (checked_ts_ == last_ts_) {
                        return UNCHANGED;
                    } else {
                        return SKEW;
                    }
                } else {
                    return CREATED;
                }
            }
        }
    
        void update_timestamp () { last_ts_ = checked_ts_; }
    
        int check_and_update_timestamp ()
        {
            const int ret = is_timestamp_updated ();
            update_timestamp ();
            return ret;
        }

        const char* filepath () const { return file_path_.c_str(); }
    private:
        std::string file_path_;
        time_t checked_ts_;
        time_t last_ts_;    
};

typedef FileWatcher file_watcher_t;

#endif
