// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#include <numa.h>

#include <sstream>
#include <stdexcept>

#ifndef INCL_THREADING_H
    #include "threading.h"
#endif

#ifndef INCL_WORKITEMS_H
    #include "workitems.h"
#endif

#ifndef __ELEVELDB_DETAIL_HPP
    #include "detail.hpp"
#endif

namespace eleveldb {

void *eleveldb_write_thread_worker(void *args);


/**
 * Meta / managment data related to a worker thread.
 */
struct ThreadData
{
    pthread_t m_Tid;                     //!< pthread id
    volatile uint32_t m_Available;       //!< 1 if thread waiting, using standard type for atomic operation
    class NumaPool & m_Pool; //!< parent pool object
    volatile eleveldb::WorkTask * m_DirectWork; //!< work passed direct to thread

    pthread_mutex_t m_Mutex;             //!< mutex for condition variable
    pthread_cond_t m_Condition;          //!< condition for thread waiting


    ThreadData(class NumaPool & Pool)
    :  m_Available(0), m_Pool(Pool), m_DirectWork(NULL)
    {
        pthread_mutex_init(&m_Mutex, NULL);
        pthread_cond_init(&m_Condition, NULL);

        return;
    }   // ThreadData

private:
    ThreadData();

};  // class ThreadData


NumaPool::NumaPool(
    eleveldb_thread_pool & Parent,
    const size_t thread_pool_size,
    cpu_set_t Set)
    : m_Parent(Parent), m_CpuSet(Set),
      work_queue_pending(&work_queue_lock),
      work_queue_atomic(0)
{
    if(false == grow_thread_pool(thread_pool_size))
        throw std::runtime_error("cannot resize thread pool");
}


NumaPool::~NumaPool()
{
    drain_thread_pool();   // all kids out of the pool
}



bool                           // returns true if available worker thread found and claimed
NumaPool::FindWaitingThread(
    eleveldb::WorkTask * work) // non-NULL to pass current work directly to a thread,
                               // NULL to potentially nudge an available worker toward backlog queue
 {
     bool ret_flag;
     size_t start, index, pool_size;

     ret_flag=false;

     // pick "random" place in thread list.  hopefully
     //  list size is prime number.
     pool_size=threads.size();
     start=(size_t)pthread_self() % pool_size;
     index=start;

     do
     {
         // perform quick test to see thread available
         if (0!=threads[index]->m_Available)
         {
             // perform expensive compare and swap to potentially
             //  claim worker thread (this is an exclusive claim to the worker)
             ret_flag = eleveldb::compare_and_swap(&threads[index]->m_Available, 1, 0);

             // the compare/swap only succeeds if worker thread is sitting on
             //  pthread_cond_wait ... or is about to be there but is holding
             //  the mutex already
             if (ret_flag)
             {

                 // man page says mutex lock optional, experience in
                 //  this code says it is not.  using broadcast instead
                 //  of signal to cover one other race condition
                 //  that should never happen with single thread waiting.
                 pthread_mutex_lock(&threads[index]->m_Mutex);
                 threads[index]->m_DirectWork=work;
                 pthread_cond_broadcast(&threads[index]->m_Condition);
                 pthread_mutex_unlock(&threads[index]->m_Mutex);
             }   // if
         }   // if

         index=(index+1)%pool_size;

     } while(index!=start && !ret_flag);

     return(ret_flag);

 }   // NumaPool::FindWaitingThread


 bool NumaPool::submit(eleveldb::WorkTask* item)
 {
     bool ret_flag(false);

     if (NULL!=item)
     {
         item->RefInc();

         if(m_Parent.shutdown_pending())
         {
             item->RefDec();
             ret_flag=false;
         }   // if

         // try to give work to a waiting thread first
         else if (!FindWaitingThread(item))
         {
             // no waiting threads, put on backlog queue
             {
                 leveldb::MutexLock lock(&work_queue_lock);
                 eleveldb::inc_and_fetch(&work_queue_atomic);
                 work_queue.push_back(item);
             }

             // to address race condition, thread might be waiting now
             FindWaitingThread(NULL);

             m_Parent.perf()->Inc(leveldb::ePerfElevelQueued);
             ret_flag=true;
         }   // if
         else
         {
             m_Parent.perf()->Inc(leveldb::ePerfElevelDirect);
             ret_flag=true;
         }   // else
     }   // if

     return(ret_flag);

 }   // NumaPool::submit


bool eleveldb_thread_pool::submit(eleveldb::WorkTask* item)
{
     bool ret_flag(false);

     if (NULL!=item)
     {
         if (0==item->m_NumaId)
             ret_flag=m_Pool[0]->submit(item);
         else
             ret_flag=m_Pool[1]->submit(item);
     }   // if

     return(ret_flag);
}   // eleveldb_thread_pool::submit



eleveldb_thread_pool::eleveldb_thread_pool(const size_t thread_pool_size)
    : shutdown(false)
{
    cpu_set_t set;

    // At least one thread means that we don't shut threads down:
    shutdown = false;

    numa_set_preferred(0);
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    CPU_SET(1, &set);
    CPU_SET(2, &set);
    CPU_SET(3, &set);
    CPU_SET(4, &set);
    CPU_SET(5, &set);
    CPU_SET(12, &set);
    CPU_SET(13, &set);
    CPU_SET(14, &set);
    CPU_SET(15, &set);
    CPU_SET(16, &set);
    CPU_SET(17, &set);
    m_Pool[0]=new NumaPool(*this, 37, set);

    numa_set_preferred(1);
    CPU_ZERO(&set);
    CPU_SET(6, &set);
    CPU_SET(7, &set);
    CPU_SET(8, &set);
    CPU_SET(9, &set);
    CPU_SET(10, &set);
    CPU_SET(11, &set);
    CPU_SET(18, &set);
    CPU_SET(19, &set);
    CPU_SET(20, &set);
    CPU_SET(21, &set);
    CPU_SET(22, &set);
    CPU_SET(23, &set);
    m_Pool[1]=new NumaPool(*this, 37, set);

    numa_set_preferred(-1);
    numa_set_localalloc();  // this should be redundant to numa_set_preferred(-1)
}

eleveldb_thread_pool::~eleveldb_thread_pool()
{
    delete m_Pool[0];
    delete m_Pool[1];
}

// Grow the thread pool by nthreads threads:
//  may not work at this time ...
bool NumaPool::grow_thread_pool(const size_t nthreads)
{
    leveldb::MutexLock l(&threads_lock);
    ThreadData * new_thread;
    int ret_val;
    bool ret_flag=true;

    if(0 >= nthreads)
        return true;  // nothing to do, but also not failure

    if(N_THREADS_MAX < nthreads + threads.size())
        return false;


    threads.reserve(nthreads);

    for(size_t i = nthreads; i && ret_flag; --i)
    {
        new_thread=new ThreadData(*this);

        ret_val=pthread_create(&new_thread->m_Tid, NULL, eleveldb_write_thread_worker,
                               static_cast<void *>(new_thread));

        if (0==ret_val)
        {
            pthread_setaffinity_np(new_thread->m_Tid, sizeof(m_CpuSet), &m_CpuSet);
            threads.push_back(new_thread);
        }
        else
        {
            ret_flag=false;
        }   // else
    }

    return true;
}


// Shut down and destroy all threads in the thread pool:
//   does not currently work
bool NumaPool::drain_thread_pool()
{
    struct release_thread
    {
        bool state;

        release_thread()
            : state(true)
            {}

        void operator()(pthread_t tid)
            {
                if(0 != pthread_join(tid, NULL))
                    state = false;
            }

        bool operator()() const { return state; }
    } rt;

    // Signal shutdown and raise all threads:
    m_Parent.shutdown = true;
    work_queue_pending.SignalAll();

    leveldb::MutexLock l(&threads_lock);
#if 0
    while(!threads.empty())
    {
        // Rebroadcast on each invocation (workers might not see the signal otherwise):
        enif_cond_broadcast(work_queue_pending);

        rt(threads.top());
        threads.pop();
    }
#endif

    return rt();
}

bool eleveldb_thread_pool::notify_caller(eleveldb::WorkTask& work_item)
{
    ErlNifPid pid;
    bool ret_flag(true);


    // Call the work function:
    basho::async_nif::work_result result = work_item();

    if (result.is_set())
    {
        if(0 != enif_get_local_pid(work_item.local_env(), work_item.pid(), &pid))
        {
            /* Assemble a notification of the following form:
               { PID CallerHandle, ERL_NIF_TERM result } */
            ERL_NIF_TERM result_tuple = enif_make_tuple2(work_item.local_env(),
                                                         work_item.caller_ref(), result.result());

            ret_flag=(0 != enif_send(0, &pid, work_item.local_env(), result_tuple));
        }   // if
        else
        {
            ret_flag=false;
        }   // else
    }   // if

    return(ret_flag);
}

/**
 * Worker threads:  worker threads have 3 states:
 *  A. doing nothing, available to be claimed: m_Available=1
 *  B. processing work passed by Erlang thread: m_Available=0, m_DirectWork=<non-null>
 *  C. processing backlog queue of work: m_Available=0, m_DirectWork=NULL
 */
void *eleveldb_write_thread_worker(void *args)
{
    ThreadData &tdata = *(ThreadData *)args;
    NumaPool& h = tdata.m_Pool;
    eleveldb::WorkTask * submission;

    submission=NULL;

    while(!h.m_Parent.shutdown)
    {
        // is work assigned yet?
        //  check backlog work queue if not
        if (NULL==submission)
        {
            // test non-blocking size for hint (much faster)
            if (0!=h.work_queue_atomic)
            {
                // retest with locking
                leveldb::MutexLock lock(&h.work_queue_lock);
                if (!h.work_queue.empty())
                {
                    submission=h.work_queue.front();
                    h.work_queue.pop_front();
                    eleveldb::dec_and_fetch(&h.work_queue_atomic);
                    h.m_Parent.perf()->Inc(leveldb::ePerfElevelDequeued);
                }   // if
            }   // if
        }   // if


        // a work item identified (direct or queue), work it!
        //  then loop to test queue again
        if (NULL!=submission)
        {
            eleveldb_thread_pool::notify_caller(*submission);
            if (submission->resubmit())
            {
                submission->recycle();
                h.submit(submission);
            }   // if

            // resubmit will increment reference again, so
            //  always dec even in reuse case
            submission->RefDec();

            submission=NULL;
        }   // if

        // no work found, attempt to go into wait state
        //  (but retest queue before sleep due to race condition)
        else
        {
            pthread_mutex_lock(&tdata.m_Mutex);
            tdata.m_DirectWork=NULL; // safety

            // only wait if we are really sure no work pending
            if (0==h.work_queue_atomic)
	    {
                // yes, thread going to wait. set available now.
	        tdata.m_Available=1;
                pthread_cond_wait(&tdata.m_Condition, &tdata.m_Mutex);
	    }    // if

            tdata.m_Available=0;    // safety
            submission=(eleveldb::WorkTask *)tdata.m_DirectWork; // NULL is valid
            tdata.m_DirectWork=NULL;// safety

            pthread_mutex_unlock(&tdata.m_Mutex);
        }   // else
    }   // while

    return 0;

}   // eleveldb_write_thread_worker

};  // namespace eleveldb
