#include "Profiler.h"

#include <sstream>
#include <fstream>
#include <sys/time.h>

using namespace std;

using namespace eleveldb;


Profiler Profiler::instance_;

/**.......................................................................
 * Constructor.
 */
Profiler::Profiler() 
{
    resize(10);
    nAccessed_  = 0;
}

/**.......................................................................
 * Destructor.
 */
Profiler::~Profiler() 
{
    std::ostringstream os;
    os << "/tmp/riak_test_scratch/" << this << "_profile.txt";
    append(os.str());
}

void Profiler::resize(unsigned n)
{
    size_ = n;
}

void Profiler::resize(std::vector<int64_t>& v)
{
    v.resize(size_);
    for(unsigned i=0; i < size_; i++)
        v[i] = 0;
}

std::vector<int64_t>& Profiler::getCurr()
{
    mutex_.Lock();
    pthread_t id = pthread_self();
    bool first = (usecCurr_.find(id) == usecCurr_.end());
    std::vector<int64_t>& v = usecCurr_[id];
    if(first)
        resize(v);
    mutex_.Unlock();
    return v;
}

std::vector<int64_t>& Profiler::getDeltas()
{
    mutex_.Lock();
    pthread_t id = pthread_self();
    bool first = (deltas_.find(id) == deltas_.end());
    std::vector<int64_t>& v = deltas_[id];
    if(first)
        resize(v);
    mutex_.Unlock();
    return v;
}

void Profiler::start(unsigned index)
{
    if(index < size_) {
        std::vector<int64_t>& curr = getCurr();
        curr[index] = getCurrentMicroSeconds();
    }
}

void Profiler::stop(unsigned index)
{
    std::vector<int64_t>& curr   = getCurr();
    std::vector<int64_t>& deltas = getDeltas();
    if(index < size_) {
        deltas[index] += (getCurrentMicroSeconds() - curr[index]);
    }
}

void Profiler::append(std::string fileName)
{
    std::fstream outfile;                                               
    outfile.open(fileName.c_str(), std::fstream::out|std::fstream::app);
    
    mutex_.Lock();

    for(std::map<pthread_t, std::vector<int64_t> >::iterator iter = deltas_.begin();
        iter != deltas_.end(); iter++) {

        outfile << iter->first << " ";
        for(unsigned i=0; i < size_; i++) {
            outfile << iter->second.at(i);
            if(i < size_-1)
                outfile << " ";
        }
        outfile << std::endl;
    }
    
    mutex_.Unlock();

    outfile.close();
}

int64_t Profiler::getCurrentMicroSeconds()
{
#if _POSIX_TIMERS >= 200801L

struct timespec ts;

// this is rumored to be faster that gettimeofday(), and sometimes
// shift less ... someday use CLOCK_MONOTONIC_RAW

 clock_gettime(CLOCK_MONOTONIC, &ts);
 return static_cast<uint64_t>(ts.tv_sec) * 1000000 + ts.tv_nsec/1000;

#else

struct timeval tv;
gettimeofday(&tv, NULL);
return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;

#endif
}

Profiler* Profiler::get()
{
    return &instance_;
}
