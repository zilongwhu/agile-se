#ifndef __AGILE_SE_IDMAP_H__
#define __AGILE_SE_IDMAP_H__

#include <map>
#include <string>
#include <stdint.h>
#include "utils/fsint.h"

class IDMapper
{
    public:
        IDMapper() { }
        virtual ~ IDMapper() { }

        virtual bool load(const char *dir, FSInterface *fs) = 0;
        virtual bool dump(const char *dir, FSInterface *fs) = 0;
        virtual int32_t map(int32_t oid, void *info) = 0;
        virtual void remove(int32_t oid) = 0;
};

typedef IDMapper *(*IDMapper_creater)();
extern std::map<std::string, IDMapper_creater> g_id_mappers;

#define DECLARE_ID_MAPPER(mapper) IDMapper *create_id_##mapper()

#define DEFINE_ID_MAPPER(mapper) \
    IDMapper *create_id_##mapper()\
    {\
        return new mapper;\
    }

#define REGISTER_ID_MAPPER(mapper) \
    do\
    {\
        g_id_mappers[#mapper] = create_id_##mapper;\
    } while(0)

#endif
