#ifndef __AGILE_SE_SIGN_DICT_H__
#define __AGILE_SE_SIGN_DICT_H__

#include <string>
#include <vector>
#include "index/hashtable.h"
#include "fsint.h"

class SignDict
{
    public:
        struct key_t
        {
            uint32_t sign1;
            uint32_t sign2;

            bool operator == (const key_t &o) const
            {
                return sign1 == o.sign1 && sign2 == o.sign2;
            }
        };
        struct hash_fun_t
        {
            size_t operator()(const key_t &o) const
            {
                return ((((uint64_t)o.sign1) << 32) | o.sign2);
            }
        };
        struct value_t
        {
            uint32_t id;
            uint32_t offset;
            uint32_t length;
        };
        typedef HashTable<key_t, value_t, hash_fun_t> Hash;
        typedef Hash::ObjectPool ObjectPool;
    private:
        SignDict(const SignDict &);
        SignDict &operator =(const SignDict &);
    public:
        SignDict();
        virtual ~SignDict();

        int init(ObjectPool *pool, uint32_t bucket_size, uint32_t buffer_size);
        bool dump(const char *dir, FSInterface *fs = NULL) const;
        bool load(const char *dir, FSInterface *fs = NULL);

        void print_meta() const;
        const uint32_t idnum() const { return m_max_id - 1; }

        /* thread safe */
        bool find(uint64_t sign, uint32_t &id) const;
        /* do not use this func */
        bool find(uint32_t id, std::string &word) const;
        /* not thread safe */
        bool find_or_insert(uint64_t sign, const char *word, uint32_t len, uint32_t &id);
    protected:
        Hash *m_dict;
        char *m_buffer;
        uint32_t m_max_id;
        uint32_t m_buffer_pos;
        uint32_t m_buffer_size;
        std::vector<std::pair<uint32_t, uint32_t> > m_ids;
};

#endif
