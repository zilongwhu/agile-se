#ifndef __AGILE_SE_MEMINFO_H__
#define __AGILE_SE_MEMINFO_H__

class Meminfo
{
    public:
        Meminfo();

        long total() const { return m_total; }
        long free() const { return m_free; }
        long buffers() const { return m_buffers; }
        long cached() const { return m_cached; }
    private:
        long m_total;
        long m_free;
        long m_buffers;
        long m_cached;
};

#endif
