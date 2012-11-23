// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011 Basho Technologies, Inc. All Rights Reserved.
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
#include "eleveldb.h"

#include <string.h>
#include "async_nif.h"

#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/write_batch.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"

#include <set>

static ErlNifResourceType* eleveldb_db_RESOURCE;
static ErlNifResourceType* eleveldb_itr_RESOURCE;

struct eleveldb_itr_handle;

typedef struct
{
    leveldb::DB* db;
    ErlNifMutex* db_lock; // protects access to db
    leveldb::Options options;
    std::set<struct eleveldb_itr_handle*>* iters;
} eleveldb_db_handle;

struct eleveldb_itr_handle
{
    leveldb::Iterator*   itr;
    ErlNifMutex*         itr_lock; // acquire *after* db_lock if both needed
    const leveldb::Snapshot*   snapshot;
    eleveldb_db_handle* db_handle;
    bool keys_only;
};
typedef struct eleveldb_itr_handle eleveldb_itr_handle;

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ENOMEM;
static ERL_NIF_TERM ATOM_BADARG;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_EINVAL;
static ERL_NIF_TERM ATOM_CREATE_IF_MISSING;
static ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;
static ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
static ERL_NIF_TERM ATOM_MAX_OPEN_FILES;
static ERL_NIF_TERM ATOM_BLOCK_SIZE;                    /* DEPRECATED */
static ERL_NIF_TERM ATOM_SST_BLOCK_SIZE;
static ERL_NIF_TERM ATOM_BLOCK_RESTART_INTERVAL;
static ERL_NIF_TERM ATOM_ERROR_DB_OPEN;
static ERL_NIF_TERM ATOM_ERROR_DB_PUT;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS;
static ERL_NIF_TERM ATOM_FILL_CACHE;
static ERL_NIF_TERM ATOM_SYNC;
static ERL_NIF_TERM ATOM_ERROR_DB_DELETE;
static ERL_NIF_TERM ATOM_CLEAR;
static ERL_NIF_TERM ATOM_PUT;
static ERL_NIF_TERM ATOM_DELETE;
static ERL_NIF_TERM ATOM_ERROR_DB_WRITE;
static ERL_NIF_TERM ATOM_BAD_WRITE_ACTION;
static ERL_NIF_TERM ATOM_KEEP_RESOURCE_FAILED;
static ERL_NIF_TERM ATOM_ITERATOR_CLOSED;
static ERL_NIF_TERM ATOM_FIRST;
static ERL_NIF_TERM ATOM_LAST;
static ERL_NIF_TERM ATOM_NEXT;
static ERL_NIF_TERM ATOM_PREV;
static ERL_NIF_TERM ATOM_INVALID_ITERATOR;
static ERL_NIF_TERM ATOM_CACHE_SIZE;
static ERL_NIF_TERM ATOM_PARANOID_CHECKS;
static ERL_NIF_TERM ATOM_ERROR_DB_DESTROY;
static ERL_NIF_TERM ATOM_KEYS_ONLY;
static ERL_NIF_TERM ATOM_COMPRESSION;
static ERL_NIF_TERM ATOM_ERROR_DB_REPAIR;
static ERL_NIF_TERM ATOM_USE_BLOOMFILTER;

ERL_NIF_TERM parse_open_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::Options& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
    {
        if (option[0] == ATOM_CREATE_IF_MISSING)
            opts.create_if_missing = (option[1] == ATOM_TRUE);
        else if (option[0] == ATOM_ERROR_IF_EXISTS)
            opts.error_if_exists = (option[1] == ATOM_TRUE);
        else if (option[0] == ATOM_PARANOID_CHECKS)
            opts.paranoid_checks = (option[1] == ATOM_TRUE);
        else if (option[0] == ATOM_MAX_OPEN_FILES)
        {
            int max_open_files;
            if (enif_get_int(env, option[1], &max_open_files))
                opts.max_open_files = max_open_files;
        }
        else if (option[0] == ATOM_WRITE_BUFFER_SIZE)
        {
            unsigned long write_buffer_sz;
            if (enif_get_ulong(env, option[1], &write_buffer_sz))
                opts.write_buffer_size = write_buffer_sz;
        }
        else if (option[0] == ATOM_BLOCK_SIZE)
        {
            /* DEPRECATED: the old block_size atom was actually ignored. */
            unsigned long block_sz;
            enif_get_ulong(env, option[1], &block_sz); // ignore
        }
        else if (option[0] == ATOM_SST_BLOCK_SIZE)
        {
            unsigned long sst_block_sz(0);
            if (enif_get_ulong(env, option[1], &sst_block_sz))
             opts.block_size = sst_block_sz; // Note: We just set the "old" block_size option.
        }
        else if (option[0] == ATOM_BLOCK_RESTART_INTERVAL)
        {
            int block_restart_interval;
            if (enif_get_int(env, option[1], &block_restart_interval))
                opts.block_restart_interval = block_restart_interval;
        }
        else if (option[0] == ATOM_CACHE_SIZE)
        {
            unsigned long cache_sz;
            if (enif_get_ulong(env, option[1], &cache_sz))
                if (cache_sz != 0)
                    opts.block_cache = leveldb::NewLRUCache(cache_sz);
        }
        else if (option[0] == ATOM_COMPRESSION)
        {
            if (option[1] == ATOM_TRUE)
            {
                opts.compression = leveldb::kSnappyCompression;
            }
            else
            {
                opts.compression = leveldb::kNoCompression;
            }
        }
        else if (option[0] == ATOM_USE_BLOOMFILTER)
        {
            // By default, we want to use a 10-bit-per-key bloom filter on a
            // per-table basis. We only disable it if explicitly asked. Alternatively,
            // one can provide a value for # of bits-per-key.
            unsigned long bfsize = 16;
            if (option[1] == ATOM_TRUE || enif_get_ulong(env, option[1], &bfsize))
            {
                opts.filter_policy = leveldb::NewBloomFilterPolicy2(bfsize);
            }
        }
    }

    return ATOM_OK;
}

ERL_NIF_TERM parse_read_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::ReadOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
    {
        if (option[0] == ATOM_VERIFY_CHECKSUMS)
            opts.verify_checksums = (option[1] == ATOM_TRUE);
        else if (option[0] == ATOM_FILL_CACHE)
            opts.fill_cache = (option[1] == ATOM_TRUE);
    }

    return ATOM_OK;
}

ERL_NIF_TERM parse_write_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::WriteOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
    {
        if (option[0] == ATOM_SYNC)
            opts.sync = (option[1] == ATOM_TRUE);
    }

    return ATOM_OK;
}

ERL_NIF_TERM write_batch_item(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::WriteBatch& batch)
{
    int arity;
    const ERL_NIF_TERM* action;
    if (enif_get_tuple(env, item, &arity, &action) ||
        enif_is_atom(env, item))
    {
        if (item == ATOM_CLEAR)
        {
            batch.Clear();
            return ATOM_OK;
        }
        ErlNifBinary key, value;

        if (action[0] == ATOM_PUT && arity == 3 &&
            enif_inspect_binary(env, action[1], &key) &&
            enif_inspect_binary(env, action[2], &value))
        {
            leveldb::Slice key_slice((const char*)key.data, key.size);
            leveldb::Slice value_slice((const char*)value.data, value.size);
            batch.Put(key_slice, value_slice);
            return ATOM_OK;
        }

        if (action[0] == ATOM_DELETE && arity == 2 &&
            enif_inspect_binary(env, action[1], &key))
        {
            leveldb::Slice key_slice((const char*)key.data, key.size);
            batch.Delete(key_slice);
            return ATOM_OK;
        }
    }

    // Failed to match clear/put/delete; return the failing item
    return item;
}

template <typename Acc> ERL_NIF_TERM fold(ErlNifEnv* env, ERL_NIF_TERM list,
                                          ERL_NIF_TERM(*fun)(ErlNifEnv*, ERL_NIF_TERM, Acc&),
                                          Acc& acc)
{
    ERL_NIF_TERM head, tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail))
    {
        ERL_NIF_TERM result = fun(env, head, acc);
        if (result != ATOM_OK)
        {
            return result;
        }
    }

    return ATOM_OK;
}

// Free dynamic elements of iterator - acquire lock before calling
static void free_itr(eleveldb_itr_handle* itr_handle)
{
    if (itr_handle->itr)
    {
        delete itr_handle->itr;
        itr_handle->itr = 0;
        itr_handle->db_handle->db->ReleaseSnapshot(itr_handle->snapshot);
    }
}

// Free dynamic elements of database - acquire lock before calling
static void free_db(eleveldb_db_handle* db_handle)
{
    if (db_handle->db)
    {
        // shutdown all the iterators - grab the lock as
        // another thread could still be in eleveldb:fold
        // which will get {error, einval} returned next time
        for (std::set<eleveldb_itr_handle*>::iterator iters_it = db_handle->iters->begin();
             iters_it != db_handle->iters->end();
             ++iters_it)
        {
            eleveldb_itr_handle* itr_handle = *iters_it;
            enif_mutex_lock(itr_handle->itr_lock);
            free_itr(*iters_it);
            enif_mutex_unlock(itr_handle->itr_lock);
        }

        // close the db
        delete db_handle->db;
        db_handle->db = NULL;

        // delete the iters
        delete db_handle->iters;
        db_handle->iters = NULL;

        // Release any cache we explicitly allocated when setting up options
        if (db_handle->options.block_cache)
        {
            delete db_handle->options.block_cache;
        }

        // Clean up any filter policies
        if (db_handle->options.filter_policy)
        {
            delete db_handle->options.filter_policy;
        }
    }
}

ERL_NIF_TERM error_tuple(ErlNifEnv* env, ERL_NIF_TERM error, leveldb::Status& status)
{
    ERL_NIF_TERM reason = enif_make_string(env, status.ToString().c_str(),
                                           ERL_NIF_LATIN1);
    return enif_make_tuple2(env, ATOM_ERROR,
                            enif_make_tuple2(env, error, reason));
}

ASYNC_NIF_DECL(eleveldb_open,
 { // args struct

    char filename[1024];
    leveldb::Options opts;
 },
 { // pre

    if (!(enif_get_string(env, argv[0], args->filename,
                          sizeof(args->filename), ERL_NIF_LATIN1)
          && enif_is_list(env, argv[1]))) {
      ASYNC_NIF_RETURN_BADARG();
    }

    // Parse out the options
    ERL_NIF_TERM result = fold(env, argv[1], parse_open_option, args->opts);
    if (result != ATOM_OK) {
      ASYNC_NIF_PRE_RETURN_CLEANUP();
      return enif_make_tuple2(env, ATOM_ERROR, result);
    }
 },
 { // work

   // Open the database
   leveldb::DB* db;
   leveldb::Status status = leveldb::DB::Open(args->opts, args->filename, &db);
   if (!status.ok()) {
     ASYNC_NIF_REPLY(error_tuple(env, ATOM_ERROR_DB_OPEN, status));
   }

   // Setup handle
   eleveldb_db_handle* handle =
     (eleveldb_db_handle*) enif_alloc_resource(eleveldb_db_RESOURCE,
                                               sizeof(eleveldb_db_handle));
   memset(handle, '\0', sizeof(eleveldb_db_handle));
   handle->db = db;
   handle->db_lock = enif_mutex_create((char*)"eleveldb_db_lock");
   handle->options = args->opts;
   handle->iters = new std::set<struct eleveldb_itr_handle*>();
   ERL_NIF_TERM result = enif_make_resource(env, handle);
   enif_release_resource(handle);
   ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
 },
 { // post

   // Nothing to dealloc/cleanup, opts is referenced by handle and dealloc'ed
   // in free_db()
 }
);

ASYNC_NIF_DECL(eleveldb_close,
 { // args struct

   eleveldb_db_handle* db_handle;
 },
 { // pre
   if (!(enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&(args->db_handle)))) {
     ASYNC_NIF_RETURN_BADARG();
   }

   enif_keep_resource(args->db_handle);
 },
 { // work

   ERL_NIF_TERM result;

   enif_mutex_lock(args->db_handle->db_lock);
   if (args->db_handle->db) {
     free_db(args->db_handle);
     result = ATOM_OK;
   } else {
     result = enif_make_tuple2(env, ATOM_ERROR, ATOM_EINVAL);
   }
   enif_mutex_unlock(args->db_handle->db_lock);
   ASYNC_NIF_REPLY(result);
 },
 { // post

   enif_release_resource(args->db_handle);
 }
);

ASYNC_NIF_DECL(eleveldb_get,
 { // args struct

   eleveldb_db_handle* db_handle;
   leveldb::ReadOptions opts;
   ErlNifBinary key;
 },
 { // pre

   if (!(enif_get_resource(env, argv[0], eleveldb_db_RESOURCE,
                           (void**)&args->db_handle) &&
         enif_inspect_binary(env, argv[1], &args->key) &&
         enif_is_list(env, argv[2]))) {
     ASYNC_NIF_RETURN_BADARG();
   }

   // Parse out the read options
   ERL_NIF_TERM result = fold(env, argv[2], parse_read_option, args->opts);
   if (result != ATOM_OK) {
     ASYNC_NIF_PRE_RETURN_CLEANUP();
     return enif_make_tuple2(env, ATOM_ERROR, result);
   }

   // Retain the handle
   enif_keep_resource(args->db_handle);
 },
 { // work

   enif_mutex_lock(args->db_handle->db_lock);
   if (args->db_handle->db == NULL) {
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_EINVAL));
    } else {
       leveldb::DB* db = args->db_handle->db;
       leveldb::Slice key_slice((const char*)args->key.data, args->key.size);
       std::string sval;
       leveldb::Status status = db->Get(args->opts, key_slice, &sval);
       if (status.ok()) {
         const size_t size = sval.size();
         ERL_NIF_TERM value_bin;
         unsigned char* value = enif_make_new_binary(env, size, &value_bin);
         if (!value) {
           enif_mutex_unlock(args->db_handle->db_lock);
           ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ENOMEM));
         } else {
           memcpy(value, sval.data(), size);
           enif_mutex_unlock(args->db_handle->db_lock);
           ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, value_bin));
         }
       } else {
         enif_mutex_unlock(args->db_handle->db_lock);
         ASYNC_NIF_REPLY(ATOM_NOT_FOUND);
       }
   }
 },
 { // post

   enif_release_resource(args->db_handle);
 }
);

ASYNC_NIF_DECL(eleveldb_write,
 { // args struct

   eleveldb_db_handle* db_handle;
   leveldb::WriteOptions opts;
   leveldb::WriteBatch batch;
 },
 { // pre

   ERL_NIF_TERM result = ATOM_OK;

   if (!(enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&args->db_handle) &&
         enif_is_list(env, argv[1]) && // Actions
         enif_is_list(env, argv[2]))) {// Opts
     ASYNC_NIF_RETURN_BADARG();
   }

   // Parse out the write options
   if ((result = fold(env, argv[2], parse_write_option, args->opts)) != ATOM_OK) {
     ASYNC_NIF_PRE_RETURN_CLEANUP();
     return enif_make_tuple2(env, ATOM_ERROR, result);
   }

   // Traverse actions and build a write batch
   if ((result = fold(env, argv[1], write_batch_item, args->batch)) != ATOM_OK) {
     ASYNC_NIF_PRE_RETURN_CLEANUP();
     return enif_make_tuple2(env, ATOM_ERROR,
                             enif_make_tuple2(env, ATOM_BAD_WRITE_ACTION, result));
   }

   // Retain the handle
   enif_keep_resource(args->db_handle);
 },
 { // work

   enif_mutex_lock(args->db_handle->db_lock);
   if (args->db_handle->db == NULL) {
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_EINVAL));
   }

   // TODO: Why does the API want a WriteBatch* versus a ref?
   leveldb::Status status = args->db_handle->db->Write(args->opts, &args->batch);
   if (status.ok()) {
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(ATOM_OK);
   } else {
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(error_tuple(env, ATOM_ERROR_DB_WRITE, status));
   }
 },
 { // post

   enif_release_resource(args->db_handle);
 }
);

ASYNC_NIF_DECL(eleveldb_iterator,
 { // args struct

   eleveldb_db_handle* db_handle;
   unsigned int keys_only;
   leveldb::ReadOptions opts;
 },
 { // pre

   if (!(enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&args->db_handle) &&
         enif_is_list(env, argv[1]))) { // Options
     ASYNC_NIF_RETURN_BADARG();
   }

   // Parse out the read options
   fold(env, argv[1], parse_read_option, args->opts);

   // Check for keys_only iterator flag
   args->keys_only = ((argc == 3) && (argv[2] == ATOM_KEYS_ONLY));

   // Retain the handle
   enif_keep_resource(args->db_handle);
 },
 { // work

   enif_mutex_lock(args->db_handle->db_lock);
   if (args->db_handle->db == NULL) {
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_EINVAL));
   } else {

     // Increment references to db_handle for duration of the iterator
     // Yes, that means we keep this twice, once for the pre, once here.
     enif_keep_resource(args->db_handle);

     // Setup handle
     eleveldb_itr_handle* itr_handle =
       (eleveldb_itr_handle*) enif_alloc_resource(eleveldb_itr_RESOURCE,
                                                  sizeof(eleveldb_itr_handle));
     memset(itr_handle, '\0', sizeof(eleveldb_itr_handle));

     // Initialize itr handle
     // TODO: Can LevelDB provide repeatable-read isolation for an
     //       iterator outside of a snapshot?  Dunno yet... find out.
     itr_handle->itr_lock = enif_mutex_create((char*)"eleveldb_itr_lock");
     itr_handle->db_handle = args->db_handle;
     itr_handle->snapshot = args->db_handle->db->GetSnapshot();
     args->opts.snapshot = itr_handle->snapshot;
     itr_handle->itr = args->db_handle->db->NewIterator(args->opts);
     itr_handle->keys_only = args->keys_only;

     ERL_NIF_TERM result = enif_make_resource(env, itr_handle);
     enif_release_resource(itr_handle);

     args->db_handle->iters->insert(itr_handle);
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
   }
 },
 { // post

   enif_release_resource(args->db_handle);
 }
);

static ERL_NIF_TERM slice_to_binary(ErlNifEnv* env, leveldb::Slice s)
{
    ERL_NIF_TERM result;
    unsigned char* value = enif_make_new_binary(env, s.size(), &result);
    memcpy(value, s.data(), s.size());
    return result;
}

ASYNC_NIF_DECL(eleveldb_iterator_move,
 { // args struct

    eleveldb_itr_handle* itr_handle;
    ERL_NIF_TERM op;
    ErlNifBinary key;
 },
 { // pre

   if (!(enif_get_resource(env, argv[0], eleveldb_itr_RESOURCE, (void**)&args->itr_handle))) {
     ASYNC_NIF_RETURN_BADARG();
   }

   // Cursor operation or key<<>>
   args->op = 0;
   if (enif_is_binary(env, argv[1])) {
     enif_inspect_binary(env, argv[1], &args->key);
   } else if (enif_is_atom(env, argv[1])) {
     args->op = argv[1];
   } else {
     ASYNC_NIF_RETURN_BADARG();
   }

   // Retain the handle
   enif_keep_resource(args->itr_handle);
 },
 { // work

   leveldb::Iterator* itr = args->itr_handle->itr;

   if (itr == NULL) {
     enif_mutex_unlock(args->itr_handle->itr_lock);
     ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATOR_CLOSED));
   } else {

     if (args->op) {
       if (args->op == ATOM_FIRST) {
         itr->SeekToFirst();
       } else if (args->op == ATOM_LAST) {
         itr->SeekToLast();
        } else if (args->op == ATOM_NEXT && itr->Valid()) {
         itr->Next();
        } else if (args->op == ATOM_PREV && itr->Valid()) {
         itr->Prev();
        }
     } else {
       leveldb::Slice key_slice((const char*)args->key.data, args->key.size);
       itr->Seek(key_slice);
     }

     ERL_NIF_TERM result;
     if (itr->Valid()) {
       if (args->itr_handle->keys_only) {
         result = enif_make_tuple2(env, ATOM_OK,
                                   slice_to_binary(env, itr->key()));
       } else {
         result = enif_make_tuple3(env, ATOM_OK,
                                   slice_to_binary(env, itr->key()),
                                   slice_to_binary(env, itr->value()));
       }
     } else {
       result = enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
     }
     enif_mutex_unlock(args->itr_handle->itr_lock);
     ASYNC_NIF_REPLY(result);
   }
 },
 { // post

   enif_release_resource(args->itr_handle);
 }
);

ASYNC_NIF_DECL(eleveldb_iterator_close,
 { // args struct
   
   eleveldb_itr_handle* itr_handle;
 },
 { // pre

   if (!(enif_get_resource(env, argv[0], eleveldb_itr_RESOURCE, (void**)&args->itr_handle))) {
     ASYNC_NIF_RETURN_BADARG();
   }

   // Retain the handle
   enif_keep_resource(args->itr_handle);
 },
 { // work

   // Make sure locks are acquired in the same order to close/free_db
   // to avoid a deadlock.
   
   enif_mutex_lock(args->itr_handle->db_handle->db_lock);
   enif_mutex_lock(args->itr_handle->itr_lock);

   if (args->itr_handle->db_handle->iters) {
     // db may have been closed before the iter (the unit test
     // does an evil close-inside-fold)
     args->itr_handle->db_handle->iters->erase(args->itr_handle);
   }
   free_itr(args->itr_handle);

   enif_mutex_unlock(args->itr_handle->itr_lock);
   enif_mutex_unlock(args->itr_handle->db_handle->db_lock);

   // matches keep in eleveldb_iterator()
   enif_release_resource(args->itr_handle->db_handle);

   ASYNC_NIF_REPLY(ATOM_OK);
 },
 { // post

   enif_release_resource(args->itr_handle);
 }
);

ASYNC_NIF_DECL(eleveldb_status,
 { // args struct
   
   eleveldb_db_handle* db_handle;
   ErlNifBinary name_bin;
 },
 { // pre

   if (!(enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&args->db_handle) &&
         enif_inspect_binary(env, argv[1], &args->name_bin))) {
     ASYNC_NIF_RETURN_BADARG();
   }

   // Retain the handle
   enif_keep_resource(args->db_handle);
 },
 { // work

   enif_mutex_lock(args->db_handle->db_lock);
   if (args->db_handle->db == NULL) {
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_EINVAL));
   } else {
     leveldb::Slice name((const char*)args->name_bin.data, args->name_bin.size);
     std::string value;
     if (args->db_handle->db->GetProperty(name, &value)) {
       ERL_NIF_TERM result;
       unsigned char* result_buf = enif_make_new_binary(env, value.size(), &result);
       memcpy(result_buf, value.c_str(), value.size());
       enif_mutex_unlock(args->db_handle->db_lock);
       ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_OK, result));
     } else {
       enif_mutex_unlock(args->db_handle->db_lock);
       ASYNC_NIF_REPLY(ATOM_ERROR);
     }
   }
 },
 { // post

   enif_release_resource(args->db_handle);
 }
);

ASYNC_NIF_DECL(eleveldb_repair,
 { // args struct

   char name[4096];
   leveldb::Options opts;
 },
 { // pre

   if (!(enif_get_string(env, argv[0], args->name, sizeof(args->name), ERL_NIF_LATIN1))) {
     ASYNC_NIF_RETURN_BADARG();
   }

   // Parse out the options
   //fold(env, argv[1], parse_repair_option, opts);
 },
 { // work

   leveldb::Status status = leveldb::RepairDB(args->name, args->opts);
   if (!status.ok()) {
     ASYNC_NIF_REPLY(error_tuple(env, ATOM_ERROR_DB_REPAIR, status));
   } else {
     ASYNC_NIF_REPLY(ATOM_OK);
   }
 },
 { // post
 }
);

ASYNC_NIF_DECL(eleveldb_destroy,
 { // args struct

   char name[4096];
   leveldb::Options opts;
 },
 { // pre

   if (!(enif_get_string(env, argv[0], args->name, sizeof(args->name), ERL_NIF_LATIN1) &&
         enif_is_list(env, argv[1]))) {
     ASYNC_NIF_RETURN_BADARG();
   }

   // Parse out the options
   fold(env, argv[1], parse_open_option, args->opts);
 },
 { // work

   leveldb::Status status = leveldb::DestroyDB(args->name, args->opts);
   if (!status.ok()) {
     ASYNC_NIF_REPLY(error_tuple(env, ATOM_ERROR_DB_DESTROY, status));
   } else {
     ASYNC_NIF_REPLY(ATOM_OK);
   }
 },
 { // post
 }
);

ASYNC_NIF_DECL(eleveldb_is_empty,
 { // args struct

   eleveldb_db_handle* db_handle;
 },
 { // pre

   if (!(enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&args->db_handle))) {
     ASYNC_NIF_RETURN_BADARG();
   }

   // Retain the handle
   enif_keep_resource(args->db_handle);
 },
 { // work

   enif_mutex_lock(args->db_handle->db_lock);
   if (args->db_handle->db == NULL) {
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(enif_make_tuple2(env, ATOM_ERROR, ATOM_EINVAL));
   } else {
     leveldb::ReadOptions opts;
     leveldb::Iterator* itr = args->db_handle->db->NewIterator(opts);
     itr->SeekToFirst();
     ERL_NIF_TERM result;
     if (itr->Valid())
       result = ATOM_FALSE;
     else
       result = ATOM_TRUE;
     delete itr;
     enif_mutex_unlock(args->db_handle->db_lock);
     ASYNC_NIF_REPLY(result);
   }
 },
 { // post

   enif_release_resource(args->db_handle);
 }
);

static void eleveldb_db_resource_cleanup(ErlNifEnv* env, void* arg)
{
    // Delete any dynamically allocated memory stored in eleveldb_db_handle
    eleveldb_db_handle* handle = (eleveldb_db_handle*)arg;

    free_db(handle);

    enif_mutex_destroy(handle->db_lock);
}

static void eleveldb_itr_resource_cleanup(ErlNifEnv* env, void* arg)
{
    // Delete any dynamically allocated memory stored in eleveldb_itr_handle
    eleveldb_itr_handle* itr_handle = (eleveldb_itr_handle*)arg;

    // No need to lock iter - it's the last reference
    if (itr_handle->itr != 0)
    {
        enif_mutex_lock(itr_handle->db_handle->db_lock);

        if (itr_handle->db_handle->iters)
        {
            itr_handle->db_handle->iters->erase(itr_handle);
        }
        free_itr(itr_handle);

        enif_mutex_unlock(itr_handle->db_handle->db_lock);
        enif_release_resource(itr_handle->db_handle);  // matches keep in eleveldb_iterator()
    }

    enif_mutex_destroy(itr_handle->itr_lock);
}

#define ATOM(Id, Value) { Id = enif_make_atom(env, Value); }

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    eleveldb_db_RESOURCE = enif_open_resource_type(env, NULL, "eleveldb_db_resource",
                                                    &eleveldb_db_resource_cleanup,
                                                    flags, NULL);
    eleveldb_itr_RESOURCE = enif_open_resource_type(env, NULL, "eleveldb_itr_resource",
                                                     &eleveldb_itr_resource_cleanup,
                                                     flags, NULL);

    // Initialize common atoms
    ATOM(ATOM_OK, "ok");
    ATOM(ATOM_BADARG, "badarg");
    ATOM(ATOM_ENOMEM, "enomem");
    ATOM(ATOM_ERROR, "error");
    ATOM(ATOM_EINVAL, "einval");
    ATOM(ATOM_TRUE, "true");
    ATOM(ATOM_FALSE, "false");
    ATOM(ATOM_CREATE_IF_MISSING, "create_if_missing");
    ATOM(ATOM_ERROR_IF_EXISTS, "error_if_exists");
    ATOM(ATOM_WRITE_BUFFER_SIZE, "write_buffer_size");
    ATOM(ATOM_MAX_OPEN_FILES, "max_open_files");
    ATOM(ATOM_BLOCK_SIZE, "block_size");
    ATOM(ATOM_SST_BLOCK_SIZE, "sst_block_size");
    ATOM(ATOM_BLOCK_RESTART_INTERVAL, "block_restart_interval");
    ATOM(ATOM_ERROR_DB_OPEN,"db_open");
    ATOM(ATOM_ERROR_DB_PUT, "db_put");
    ATOM(ATOM_NOT_FOUND, "not_found");
    ATOM(ATOM_VERIFY_CHECKSUMS, "verify_checksums");
    ATOM(ATOM_FILL_CACHE,"fill_cache");
    ATOM(ATOM_SYNC, "sync");
    ATOM(ATOM_ERROR_DB_DELETE, "db_delete");
    ATOM(ATOM_CLEAR, "clear");
    ATOM(ATOM_PUT, "put");
    ATOM(ATOM_DELETE, "delete");
    ATOM(ATOM_ERROR_DB_WRITE, "db_write");
    ATOM(ATOM_BAD_WRITE_ACTION, "bad_write_action");
    ATOM(ATOM_KEEP_RESOURCE_FAILED, "keep_resource_failed");
    ATOM(ATOM_ITERATOR_CLOSED, "iterator_closed");
    ATOM(ATOM_FIRST, "first");
    ATOM(ATOM_LAST, "last");
    ATOM(ATOM_NEXT, "next");
    ATOM(ATOM_PREV, "prev");
    ATOM(ATOM_INVALID_ITERATOR, "invalid_iterator");
    ATOM(ATOM_CACHE_SIZE, "cache_size");
    ATOM(ATOM_PARANOID_CHECKS, "paranoid_checks");
    ATOM(ATOM_ERROR_DB_DESTROY, "error_db_destroy");
    ATOM(ATOM_ERROR_DB_REPAIR, "error_db_repair");
    ATOM(ATOM_KEYS_ONLY, "keys_only");
    ATOM(ATOM_COMPRESSION, "compression");
    ATOM(ATOM_USE_BLOOMFILTER, "use_bloomfilter");

    ASYNC_NIF_LOAD();
    return 0;
}

static void on_unload(ErlNifEnv* env, void* priv_data)
{
  ASYNC_NIF_UNLOAD()
}

static int on_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
  ASYNC_NIF_UPGRADE()
  return 0;
}

static ErlNifFunc nif_funcs[] =
{
    {"open_nif", 3, eleveldb_open},
    {"close_nif", 2, eleveldb_close},
    {"get_nif", 4, eleveldb_get},
    {"write_nif", 4, eleveldb_write},
    {"iterator_nif", 3, eleveldb_iterator},
    {"iterator_nif", 4, eleveldb_iterator},
    {"iterator_move_nif", 3, eleveldb_iterator_move},
    {"iterator_close_nif", 2, eleveldb_iterator_close},
    {"status_nif", 3, eleveldb_status},
    {"destroy_nif", 3, eleveldb_destroy},
    {"repair_nif", 3, eleveldb_repair},
    {"is_empty_nif", 2, eleveldb_is_empty},
};

extern "C" {
    ERL_NIF_INIT(eleveldb, nif_funcs, &on_load, NULL, &on_upgrade, &on_unload)
}
