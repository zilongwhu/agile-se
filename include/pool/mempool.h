#ifndef __AGILE_SE_MEMPOOL_H__
#define __AGILE_SE_MEMPOOL_H__

#include <stdint.h>
#include <vector>
#include <algorithm>
#include <ext/hash_map>
#include "log_utils.h"

#ifndef AGILE_SE_MEM_PAGE_SIZE
#define AGILE_SE_MEM_PAGE_SIZE   (1024*1024u)
#endif

#ifndef UINT32_MAX
#define UINT32_MAX		(4294967295U)
#endif

class Mempool
{
    public:
        typedef uint32_t vaddr_t;
    private:
        typedef __gnu_cxx::hash_map<uint32_t, uint32_t> Map;
    private:
        struct SlabMeta
        {
            uint32_t elem_size;
            uint32_t page_size;
            uint32_t slab_index;
            uint32_t page_bits;
            uint32_t offset_bits;
        };
        struct Slab
        {
            SlabMeta meta;
            vaddr_t freelist;
            uint32_t free_num;
            uint32_t block_num;
            uint32_t cur_block_no;
        };
        struct Block
        {
            SlabMeta meta;
            uint32_t alloced_num;
            uint32_t cur_page_no;
            uint32_t cur_offset;
            void **pages;
        };
    private:
        Mempool(const Mempool &);
        Mempool &operator =(const Mempool &);
    public:
        Mempool() { }
        ~Mempool() { this->clear(); }

        int register_item(uint32_t elem_size)
        {
            if (elem_size < sizeof(vaddr_t))
            {
                P_WARNING("too small elem size=%u", elem_size);
                return -1;
            }
            if (elem_size > AGILE_SE_MEM_PAGE_SIZE)
            {
                P_WARNING("too large elem size=%u", elem_size);
                return -1;
            }
            for (size_t i = 0; i < m_items.size(); ++i)
            {
                if (m_items[i] == elem_size)
                {
                    return 1;
                }
            }
            m_items.push_back(elem_size);
            P_WARNING("register item: elem size=%u", elem_size);
            return 0;
        }

        int init(uint32_t max_items_num)
        {
            if (max_items_num <= 0)
            {
                P_WARNING("invalid max_items_num=%u", max_items_num);
                return -1;
            }
            if (m_items.size() == 0)
            {
                P_WARNING("register nothing, register some item first");
                return -1;
            }
            std::sort(m_items.begin(), m_items.end());
            /**************************************************
             *    first     *     second      *    third      *
             **************************************************/
            uint32_t second = log2(max_items_num);
            uint32_t first = 32 - second;

            P_WARNING("block occupies %u bits", first);
            m_block_bits = first;
            m_blocks.reserve((1u << first));
            m_slabs.resize(m_items.size());
            for (size_t i = 0; i < m_items.size(); ++i)
            {
                uint32_t elem_size = m_items[i];
                uint32_t page_size = AGILE_SE_MEM_PAGE_SIZE;
                uint32_t elems_num = page_size / elem_size;

                uint32_t third = log2(elems_num);
                if (third > second)
                {
                    third = second;
                }

                page_size = (1u << third) * elem_size;

                m_slabs[i].meta.elem_size = elem_size;
                m_slabs[i].meta.page_size = page_size;
                m_slabs[i].meta.slab_index = i;
                m_slabs[i].meta.page_bits = second - third;
                m_slabs[i].meta.offset_bits = third;
                m_slabs[i].freelist = 0;
                m_slabs[i].free_num = 0;
                m_slabs[i].block_num = 0;
                m_slabs[i].cur_block_no = UINT32_MAX;

                m_size2off[elem_size] = i;

                P_WARNING("slab[%d]:", i);
                P_WARNING("    elem_size  : %u", m_slabs[i].meta.elem_size);
                P_WARNING("    page_size  : %u", m_slabs[i].meta.page_size);
                P_WARNING("    slab_index : %u", m_slabs[i].meta.slab_index);
                P_WARNING("    page_bits  : %u", m_slabs[i].meta.page_bits);
                P_WARNING("    offset_bits: %u", m_slabs[i].meta.offset_bits);
            }
            m_size2off_end = m_size2off.end();
            P_WARNING("init ok");
            return 0;
        }

        vaddr_t alloc(uint32_t elem_size)
        {
            Slab *p_slab = NULL;
            {
                Map::iterator it = m_size2off.find(elem_size);
                if (it == m_size2off_end)
                {
                    P_WARNING("unregistered elem size: %u", elem_size);
                    return 0;
                }
                p_slab = &m_slabs[it->second];
            }
            if (UINT32_MAX == p_slab->cur_block_no) /* not init yet */
            {
                if (!this->alloc_one_block(p_slab))
                {
                    P_WARNING("failed to alloc new block, elem size: %u", elem_size);
                    return 0;
                }
            }
            vaddr_t ret = p_slab->freelist;
            if (ret)
            {
                void *ptr = this->addr(ret);
                p_slab->freelist = *(uint32_t *)ptr;
                --p_slab->free_num;
            }
            else
            {
                Block *p_block = &m_blocks[p_slab->cur_block_no];
                if (p_block->alloced_num >= (1u << (32 - m_block_bits))) /* cur block full */
                {
                    if (!this->alloc_one_block(p_slab))
                    {
                        P_WARNING("failed to alloc new block, elem size: %u", elem_size);
                        return 0;
                    }
                    p_block = &m_blocks[p_slab->cur_block_no];
                }
                if (p_block->cur_offset >= (1u << p_slab->meta.offset_bits)) /* cur page full */
                {
                    p_block->cur_offset = 0;
                    ++p_block->cur_page_no;
                }
                if (NULL == p_block->pages[p_block->cur_page_no]) /* try alloc a new page */
                {
                    p_block->pages[p_block->cur_page_no] = ::malloc(p_slab->meta.page_size);
                    if (NULL == p_block->pages[p_block->cur_page_no])
                    {
                        P_WARNING("failed to alloc a page, page_size=%u, cur_page_no=%u, cur_block_no=%u, elem_size=%u",
                                p_slab->meta.page_size, p_block->cur_page_no, p_slab->cur_block_no, elem_size);
                        return 0;
                    }
                }
                ret = ((p_slab->cur_block_no << (32 - m_block_bits))
                        | (p_block->cur_page_no << p_slab->meta.offset_bits)
                        | p_block->cur_offset) + 1;
                ++p_block->cur_offset;
                ++p_block->alloced_num;
            }
            return ret;
        }

        void free(vaddr_t ptr, uint32_t /* elem_size */)
        {
            if (0 == ptr)
            {
                return ;
            }
            void *real = this->addr(ptr);
            if (NULL == real)
            {
                P_WARNING("failed to free ptr=%u", ptr);
                return ;
            }
            const uint32_t block_no = ((ptr - 1) >> (32 - m_block_bits));
            const uint32_t idx = m_blocks[block_no].meta.slab_index;
            *((uint32_t *)real) = m_slabs[idx].freelist;
            m_slabs[idx].freelist = ptr;
            ++m_slabs[idx].free_num;
        }

        void *addr(vaddr_t ptr) const
        {
            if (0 == ptr)
            {
                return NULL;
            }
            --ptr;
            const uint32_t block_no = (ptr >> (32 - m_block_bits));
            if (block_no >= m_blocks.size())
            {
                P_WARNING("invalid block no");
                return NULL;
            }
            const Block *p_block = &m_blocks[block_no];
            const uint32_t page_no = ((ptr >> p_block->meta.offset_bits) & ((1u << p_block->meta.page_bits) - 1));
            if (NULL == p_block->pages[page_no])
            {
                P_WARNING("invalid page no, page is NULL");
                return NULL;
            }
            const uint32_t offset = (ptr & ((1u << p_block->meta.offset_bits) - 1));
            return ((char *)p_block->pages[page_no]) + offset * p_block->meta.elem_size;
        }

        void clear()
        {
            for (size_t i = 0; i < m_blocks.size(); ++i)
            {
                for (size_t j = 0, page_num = (1u << m_blocks[i].meta.page_bits); j < page_num; ++j)
                {
                    if (m_blocks[i].pages[j])
                    {
                        ::free(m_blocks[i].pages[j]);
                    }
                }
                ::free(m_blocks[i].pages);
            }
            m_blocks.resize(0);
            m_blocks.reserve((1u << m_block_bits));
            for (size_t i = 0; i < m_slabs.size(); ++i)
            {
                m_slabs[i].freelist = 0;
                m_slabs[i].free_num = 0;
                m_slabs[i].block_num = 0;
                m_slabs[i].cur_block_no = UINT32_MAX;
            }
            return ;
        }

        void print_meta() const
        {
            int block_num = 0;
            size_t total_used = 0;
            for (size_t i = 0; i < m_slabs.size(); ++i)
            {
                uint32_t page_num = (1u << m_slabs[i].meta.page_bits);
                size_t mem_used = 0;

                if (m_slabs[i].block_num > 0)
                {
                    mem_used = (sizeof(Block) + (sizeof(void *) + m_slabs[i].meta.page_size) * page_num)
                        * (m_slabs[i].block_num - 1);
                    mem_used += sizeof(Block) + sizeof(void *) * page_num
                        + m_slabs[i].meta.page_size * m_blocks[m_slabs[i].cur_block_no].cur_page_no;
                }
                block_num += m_slabs[i].block_num;
                total_used += mem_used;

                P_WARNING("slab[%d]:", int(i));
                P_WARNING("    elem_size=%u, page_size=%u, page_num_per_block=%u",
                        m_slabs[i].meta.elem_size, m_slabs[i].meta.page_size, page_num);
                P_WARNING("    free_num=%u, block_num=%u", m_slabs[i].free_num, m_slabs[i].block_num);
            }
            total_used += (m_blocks.capacity() - block_num) * sizeof(Block);
            P_WARNING("block num used=%u, not used=%u, total mem used=%lu",
                    block_num, (uint32_t)(m_blocks.capacity() - block_num), (uint64_t)total_used);
        }
    private:
        static int log2(uint32_t num)
        {
            for (int i = 0; i < 32; ++i)
            {
                if ((1u << i) >= num)
                {
                    return i > 1 ? i - 1 : 1;
                }
            }
            return 31;
        }
        bool alloc_one_block(Slab *p_slab)
        {
            if (m_blocks.size() >= (1u << m_block_bits))
            {
                P_WARNING("too many blocks alloced, block num=%u", (uint32_t)m_blocks.size());
                return false;
            }
            const uint32_t block_no = m_blocks.size();
            m_blocks.resize(block_no + 1);

            uint32_t page_num = (1u << p_slab->meta.page_bits);
            void **pages = (void **)::calloc(page_num, sizeof(void *));
            if (NULL == pages)
            {
                m_blocks.resize(block_no);

                P_WARNING("failed to alloc pages array, page_num=%u, elem_size=%u, page_size=%u",
                        page_num, p_slab->meta.elem_size, p_slab->meta.page_size);
                return false;
            }
            m_blocks[block_no].meta = p_slab->meta;
            m_blocks[block_no].alloced_num = 0;
            m_blocks[block_no].cur_page_no = 0;
            m_blocks[block_no].cur_offset = 0;
            m_blocks[block_no].pages = pages;

            p_slab->cur_block_no = block_no;
            ++p_slab->block_num;
            return true;
        }
    private:
        std::vector<uint32_t> m_items;
        Map m_size2off;
        Map::iterator m_size2off_end;
        uint32_t m_block_bits;
        std::vector<Slab> m_slabs;
        std::vector<Block> m_blocks;
};

#endif
