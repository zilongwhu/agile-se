// =====================================================================================
//
//       Filename:  mempool2.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/12/2014 10:39:29 AM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_MEMORY_POOL2_H__
#define __AGILE_SE_MEMORY_POOL2_H__

#include <map>
#include <vector>
#include <utility>
#include "log.h"

class BitsSpliter
{
    public:
        BitsSpliter()
        {
            first_num = second_num = first_mask = second_mask = 0;
        }

        void init(uint32_t first, uint32_t second)
        {
            first_num = first;
            second_num = second;
            first_mask = (((1 << first) - 1) << second);
            second_mask = ((1 << second) - 1);
        }

        uint32_t index_mask() const { return first_mask; }
        uint32_t offset_mask() const { return second_mask; }

        uint32_t index_num() const { return first_num; }
        uint32_t offset_num() const { return second_num; }
    private:
        uint32_t first_num;
        uint32_t second_num;
        uint32_t first_mask;
        uint32_t second_mask;
};

class VMemoryPool
{
    public:
        typedef uint32_t vaddr_t;
        const static vaddr_t null = 0;
    private:
        const static uint32_t uint32_max = (4294967295U);
    private:
        VMemoryPool(const VMemoryPool &);
        VMemoryPool &operator =(const VMemoryPool &);
    public:

        VMemoryPool() { }
        ~VMemoryPool() { this->clear(); }

        int register_item(uint32_t elem_size, uint32_t page_size)
        {
            if (elem_size < sizeof(vaddr_t) || page_size <= 0 || elem_size > page_size)
            {
                WARNING("invalid args: elem_size=%u, page_size=%u", elem_size, page_size);
                return -1;
            }
            for (size_t i = 0; i < m_registered_items.size(); ++i)
            {
                if (m_registered_items[i].first == elem_size)
                {
                    WARNING("overwrite: elem_size=%u, old page_size=%u, new page_size=%u",
                            elem_size, m_registered_items[i].second, page_size);
                    m_registered_items[i].second = page_size;
                    return 1;
                }
            }
            m_registered_items.push_back(std::make_pair(elem_size, page_size));
            WARNING("register item: elem_size=%u, page_size=%u", elem_size, page_size);
            return 0;
        }

        int init(uint32_t max_items_num)
        {
            if (max_items_num <= 0 || m_registered_items.size() <= 0)
            {
                WARNING("invalid args: max_items_num=%u, registered items=%u",
                        max_items_num, (uint32_t)m_registered_items.size());
                return -1;
            }
            this->clear();
            WARNING("max_items_num=%u", max_items_num);

            uint32_t second = log2(max_items_num - 1);
            uint32_t first = 32 - second;
            m_bits.init(first, second);

            WARNING("m_bits: index_num=%u, offset_num=%u, index_mask=%u, offset_mask=%u",
                    m_bits.index_num(), m_bits.offset_num(),
                    m_bits.index_mask(), m_bits.offset_mask());

            m_blocks.reserve((1u << m_bits.index_num()));
            m_slabs.resize(m_registered_items.size());

            WARNING("m_blocks: capacity=%u", (uint32_t)m_blocks.capacity());
            WARNING("m_slabs: size=%u", (uint32_t)m_slabs.size());

            uint32_t total = m_bits.offset_num();
            for (size_t i = 0; i < m_registered_items.size(); ++i)
            {
                uint32_t elem_size = m_registered_items[i].first;
                uint32_t page_size = m_registered_items[i].second;
                uint32_t elems_num = page_size / elem_size;

                second = log2(elems_num - 1);
                if (second > total)
                {
                    second = total;
                }
                first = total - second;

                page_size = (1 << second) * elem_size;

                m_slabs[i].meta.bits.init(first, second);

                m_slabs[i].meta.elem_size = elem_size;
                m_slabs[i].meta.page_size = page_size;
                m_slabs[i].meta.slab_index = i;
                m_slabs[i].freelist = 0;
                m_slabs[i].free_num = 0;
                m_slabs[i].cur_block_no = uint32_max;
                m_slabs[i].block_num = 0;

                m_size2off.insert(std::make_pair(elem_size, i));

                WARNING("m_slabs[%d]:", int(i));
                WARNING("    elem_size: %u", elem_size);
                WARNING("    page_size: %u", page_size);
                WARNING("    bits: index_num=%u, offset_num=%u, index_mask=%u, offset_mask=%u",
                        m_slabs[i].meta.bits.index_num(),
                        m_slabs[i].meta.bits.offset_num(),
                        m_slabs[i].meta.bits.index_mask(),
                        m_slabs[i].meta.bits.offset_mask());
            }
            WARNING("init ok");
            return 0;
        }

        int init(const char *path, const char *file)
        {
            return 0;
        }

        vaddr_t alloc(size_t elem_size)
        {
            Slab *p_slab = NULL;
            {
                std::map<uint32_t, uint32_t>::const_iterator it = m_size2off.find(elem_size);
                if (it == m_size2off.end())
                {
                    WARNING("unregistered elem size: %u", elem_size);
                    return null;
                }
                if (it->second >= m_slabs.size())
                {
                    WARNING("internal error");
                    return null;
                }
                p_slab = &m_slabs[it->second];
            }
            if (p_slab->cur_block_no == uint32_max)
            {
                if (!this->alloc_one_block(p_slab->cur_block_no, p_slab->meta))
                {
                    WARNING("failed to alloc new block, elem_size=%u", elem_size);
                    return null;
                }
            }
            vaddr_t ret = p_slab->freelist;
            if (ret)
            {
                void *ptr = this->addr(ret);
                p_slab->freelist = *(uint32_t *)ptr;
                --p_slab->free_num;
                DEBUG("reuse from freelist ok, addr=%u, elem size: %u", ret, elem_size);
            }
            else
            {
                Block *p_block = &m_blocks[p_slab->cur_block_no];
                if (p_block->slab.offset >= (1u << m_bits.offset_num())) /* block is full */
                {
                    DEBUG("current block is full, cur_elem_no=%u, cur_page_no=%u, cur_block_no=%u",
                            p_block->cur_elem_no, p_block->cur_page_no, p_slab->cur_block_no);

                    if (!this->alloc_one_block(p_slab->cur_block_no, p_slab->meta))
                    {
                        WARNING("failed to alloc new block, elem_size=%u", elem_size);
                        return null;
                    }
                    p_block = &m_blocks[p_slab->cur_block_no];
                }
                if (p_block->cur_elem_no >= (1u << p_block->slab.meta.bits.offset_num())) /* page is full */
                {
                    DEBUG("current page is full, cur_elem_no=%u, cur_page_no=%u, cur_block_no=%u",
                            p_block->cur_elem_no, p_block->cur_page_no, p_slab->cur_block_no);

                    p_block->cur_elem_no = 0;
                    ++p_block->cur_page_no;
                }
                if (NULL == p_block->pages[p_block->cur_page_no]) /* try alloc a new page */
                {
                    DEBUG("try to alloc a new page, page_size=%u, cur_elem_no=%u, cur_page_no=%u, cur_block_no=%u",
                            p_block->slab.meta.page_size, p_block->cur_elem_no, p_block->cur_page_no, p_slab->cur_block_no);

                    p_block->pages[p_block->cur_page_no] = ::malloc(p_block->slab.meta.page_size);
                    if (NULL == p_block->pages[p_block->cur_page_no])
                    { /* failed to alloc a new page */
                        WARNING("failed to alloc a page, page_size=%u, cur_elem_no=%u, cur_page_no=%u, cur_block_no=%u",
                                p_block->slab.meta.page_size, p_block->cur_elem_no, p_block->cur_page_no, p_slab->cur_block_no);
                        return null;
                    }
                }
                ret = ((p_slab->cur_block_no << m_bits.offset_num())
                        | ((p_block->cur_page_no) << p_slab->meta.bits.offset_num())
                        | (p_block->cur_elem_no)) + 1;
                ++p_block->cur_elem_no;
                ++p_block->slab.offset;

                DEBUG("alloc ok, addr=%u, elem_size=%u, page_size=%u, cur_elem_no=%u, cur_page_no=%u, offset=%u, cur_block_no=%u",
                        ret, elem_size, p_block->slab.meta.page_size, p_block->cur_elem_no, p_block->cur_page_no, p_block->slab.offset, p_slab->cur_block_no);
            }
            return ret;
        }

        void free(vaddr_t ptr)
        {
            void *real = this->addr(ptr);
            if (NULL != real)
            {
                const uint32_t block_no = (((ptr - 1) & m_bits.index_mask()) >> m_bits.offset_num());
                const uint32_t idx = m_blocks[block_no].slab.meta.slab_index;
                *((uint32_t *)real) = m_slabs[idx].freelist;
                m_slabs[idx].freelist = ptr;
                ++m_slabs[idx].free_num;
            }
        }

        void *addr(vaddr_t ptr)
        {
            if (null == ptr)
            {
                WARNING("invalid address");
                return NULL;
            }
            const uint32_t block_no = (((ptr - 1) & m_bits.index_mask()) >> m_bits.offset_num());
            if (block_no > m_blocks.size())
            {
                WARNING("invalid block_no=%u, block size=%u", block_no, (uint32_t)m_blocks.size());
                return NULL;
            }
            const Block *p_block = &m_blocks[block_no];
            if (!p_block->pages)
            {
                WARNING("pages is NULL,  block_no=%u", block_no);
                return NULL;
            }
            const uint32_t page_no = (((ptr - 1) & m_bits.offset_mask()) >> p_block->slab.meta.bits.offset_num());
            if (!p_block->pages[page_no])
            {
                WARNING("page is NULL, page_no=%u, block_no=%u", page_no, block_no);
                return NULL;
            }
            const uint32_t elem_no = ((ptr - 1) & p_block->slab.meta.bits.offset_mask());
            return ((char *)p_block->pages[page_no]) + elem_no * p_block->slab.meta.elem_size;
        }

        void clear()
        {
            for (size_t i = 0; i < m_blocks.size(); ++i)
            {
                if (NULL == m_blocks[i].pages)
                {
                    WARNING("internal error");
                    continue;
                }
                for (size_t j = 0, page_num = (1u << m_blocks[i].slab.meta.bits.index_num()); j < page_num; ++j)
                {
                    if (m_blocks[i].pages[j])
                    {
                        ::free(m_blocks[i].pages[j]);
                    }
                }
                ::free(m_blocks[i].pages);
            }
            m_blocks.resize(0);
            for (size_t i = 0; i < m_slabs.size(); ++i)
            {
                m_slabs[i].freelist = 0;
                m_slabs[i].free_num = 0;
                m_slabs[i].cur_block_no = uint32_max;
                m_slabs[i].block_num = 0;
            }
        }

        void print_meta() const
        {
            for (size_t i = 0; i < m_slabs.size(); ++i)
            {
                WARNING("slab[%d]: elem_size=%u, page_size=%u, free_num=%u, block_num=%u",
                        int(i), m_slabs[i].meta.elem_size, m_slabs[i].meta.page_size,
                        m_slabs[i].free_num, m_slabs[i].block_num);
            }
        }
    private:
        struct SlabMeta
        {
            uint32_t elem_size;
            uint32_t page_size;
            uint32_t slab_index;

            BitsSpliter bits;
        };
        struct Slab
        {
            SlabMeta meta;
            vaddr_t freelist;
            uint32_t free_num;
            uint32_t cur_block_no;
            uint32_t block_num;
        };
        struct Block
        {
            struct
            {
                SlabMeta meta;
                uint32_t offset;
            } slab;

            void **pages;
            uint32_t cur_elem_no;
            uint32_t cur_page_no;
        };
    private:
        bool alloc_one_block(uint32_t &block_no, const SlabMeta &meta)
        {
            if (m_blocks.size() >= (1u << m_bits.index_num()))
            {
                WARNING("too many blocks, block num=%u, index num=%u",
                        (uint32_t)m_blocks.size(), m_bits.index_num());
                return false;
            }
            uint32_t page_num = (1u << meta.bits.index_num());
            void **pages = (void **)::calloc(page_num, sizeof(void *));
            if (NULL == pages)
            {
                WARNING("failed to calloc pages array, page_num=%u, elem_size=%u, page_size=%u",
                        page_num, meta.elem_size, meta.page_size);
                return false;
            }
            block_no = m_blocks.size();
            m_blocks.resize(block_no + 1);

            m_blocks[block_no].slab.meta = meta;
            m_blocks[block_no].slab.offset = 0;
            m_blocks[block_no].cur_page_no = 0;
            m_blocks[block_no].cur_elem_no = 0;
            m_blocks[block_no].pages = pages;

            ++m_slabs[meta.slab_index].block_num;

            DEBUG("alloc block: id=%u, block_num=%u, page_num=%u, elem_size=%u, page_size=%u",
                    block_no, m_slabs[meta.slab_index].block_num, page_num, meta.elem_size, meta.page_size);
            return true;
        }

        static int log2(uint32_t num)
        {
            int bits = 0;
            while (num > 0)
            {
                ++bits;
                num >>= 1;
            }
            return bits;
        }
    private:
        std::vector<std::pair<uint32_t, uint32_t> > m_registered_items;
        std::map<uint32_t, uint32_t> m_size2off;

        std::vector<Slab> m_slabs;
        std::vector<Block> m_blocks;
        
        BitsSpliter m_bits;
};

#endif
