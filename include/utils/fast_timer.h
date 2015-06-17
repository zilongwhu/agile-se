#ifndef __AGILE_SE_FAST_TIMER_H__
#define __AGILE_SE_FAST_TIMER_H__

#include <cstddef>
#include <stdint.h>
#include <sys/time.h>

class FastTimer
{
    public:
        FastTimer()
        {
            _st.tv_sec = _et.tv_sec = 0;
            _st.tv_usec = _et.tv_usec = 0;
        }

        void start()
        {
            ::gettimeofday(&_st, NULL);
        }
        void stop()
        {
            ::gettimeofday(&_et, NULL);
        }
        int64_t stopAndStart()
        {
            this->stop();
            int64_t us = this->timeInUs();
            this->_st = this->_et;
            return us;
        }

        int64_t timeInMs() const
        {
            return this->timeInUs()/1000;
        }
        int64_t timeInUs() const
        {
            return (_et.tv_sec - _st.tv_sec)*1000000l + ((int)_et.tv_usec - (int)_st.tv_usec);
        }

        struct timeval startTime() const
        {
            return _st;
        }
        void startTime(struct timeval tm)
        {
            this->_st = tm;
        }
        struct timeval stopTime() const
        {
            return _et;
        }
        void stopTime(struct timeval tm)
        {
            this->_et = tm;
        }
    private:
        struct timeval _st;
        struct timeval _et;
};

#endif
