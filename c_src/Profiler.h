// $Id: $

#ifndef ELEVELDB_PROFILER_H
#define ELEVELDB_PROFILER_H

/**
 * @file Profiler.h
 * 
 * Tagged: Tue Oct 27 10:44:14 PDT 2015
 * 
 * @version: $Revision: $, $Date: $
 * 
 * @author /bin/bash: username: command not found
 */
#include <string>
#include <map>
#include <vector>

#include <stdint.h>

#include "mutex.h"

namespace eleveldb {

    class Profiler {
    public:

        /**
         * Destructor.
         */
        virtual ~Profiler();

        void resize(unsigned n);
        void start(unsigned index);
        void stop(unsigned index);
        void append(std::string fileName);

        static int64_t getCurrentMicroSeconds();
        static Profiler* get();

    private:

        Profiler();

        std::map<pthread_t, std::vector<int64_t> > usecCurr_;
        std::map<pthread_t, std::vector<int64_t> > deltas_;

        std::vector<int64_t>& getCurr();
        std::vector<int64_t>& getDeltas();

        void resize(std::vector<int64_t>& v);

        unsigned size_;
        unsigned nAccessed_;

        static Profiler instance_;

        Mutex mutex_;

    }; // End class Profiler

} // End namespace eleveldb



#endif // End #ifndef ELEVELDB_PROFILER_H
