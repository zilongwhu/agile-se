#ifndef __AGILE_SE_FILE_SYSTEM_INTERFACE_H__
#define __AGILE_SE_FILE_SYSTEM_INTERFACE_H__

#include <stdarg.h>
#include <stdio.h>

class FSInterface
{
    public:
        typedef void *File;

        virtual ~FSInterface() { }

        virtual File fopen(const char *filename, const char *mode) = 0;
        virtual int fclose(File file) = 0;
        virtual int fprintf(File file, const char *format, ...) = 0;
        virtual size_t fwrite(const void *ptr, size_t size, size_t nitems, File file) = 0;
        virtual size_t fread(void *ptr, size_t size, size_t nitems, File file) = 0;
};

class DefaultFS: public FSInterface
{
    public:
        static DefaultFS s_default;
    public:
        File fopen(const char *filename, const char *mode)
        {
            return ::fopen(filename, mode);
        }
        int fclose(File file)
        {
            return ::fclose((FILE *)file);
        }
        int fprintf(File file, const char *format, ...)
        {
            va_list args;
            va_start(args, format);
            int ret = ::vfprintf((FILE *)file, format, args);
            va_end(args);
            return ret;
        }
        size_t fwrite(const void *ptr, size_t size, size_t nitems, File file)
        {
            return ::fwrite(ptr, size, nitems, (FILE *)file);
        }
        size_t fread(void *ptr, size_t size, size_t nitems, File file)
        {
            return ::fread(ptr, size, nitems, (FILE *)file);
        }
};

#endif
