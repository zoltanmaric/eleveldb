%% -------------------------------------------------------------------
%%
%% fault injection primitives
%%
%% Copyright (c) 2014 Basho Technologies, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(trigger_commonpaths).
-export([config/0]).

-include("faulterl.hrl").

config() ->
    DoOnlyOnceHack = "
#ifndef TRIGGER_COMMONPATHS
#define TRIGGER_COMMONPATHS
static char *interesting_strings[] = {
    \".log\",
    \"CURRENT\",
    \"LOCK\",
    \"LOG\",
    \"MANIFEST\",
    \"sst_\",
    \".db\",
    NULL
};

static int flock_op_array[] = { LOCK_SH, LOCK_EX };
static int open_write_op_array[] = { O_WRONLY, O_RDWR };
#endif
",

    %% MEMO: PathC_ means "path checking via strstr"
    PathC_CHeaders = ["<string.h>", "<stdio.h>"],
    PathC_TStruct = "
typedef struct {
    char *i_name; /* Must be present */
}",

    PathC_TNewInstanceArgs = "char *strstr_1st_arg",
    PathC_TNewInstanceBody = "
",
    %% NOTE: 'path' below matches last arg to config()
    PathC_TBody = "
    int i;

    res = 0;
    for (i = 0; interesting_strings[i] != NULL; i++) {
        if (strstr(path, interesting_strings[i]) != NULL) {
            res++;
            break;
        }
    }
",

    %% Memo: BitC_ means "bit checking", e.g. open(2) flags, flock(2) operation
    BitC_CHeaders = ["<sys/file.h>"],
    BitC_TStruct = "
typedef struct {
    char *i_name;  /* Must be present */
    int array[11]; /* Array of bit patterns to check, 0 signals end of array */
}",
    BitC_TNewInstanceArgs = "int *array",
    BitC_TNewInstanceBody = "
    int i;

    for (i = 0; array[i] != 0 && i < sizeof(a->array); i++) {
        a->array[i] = array[i];
    }
    array[i] = 0;
",
    %% NOTE: 'path' below matches last arg to config()
    BitC_TBody = "
    int i;

    res = 0;
    for (i = 0; a->array[i] != 0; i++) {
        if (a->array[i] & checked_operation) {
            res++;
            break;
        }
    }
",

    [trigger_args_of_intercept:config(
         PathC_CHeaders, [DoOnlyOnceHack], [],
         true, true, false,
         PathC_TStruct, "", PathC_TNewInstanceArgs, PathC_TNewInstanceBody,
         InterceptName, PathC_TBody,
         InterceptName, "path") || InterceptName <- ["access", "open",
                                                     "unlink", "unlinkat"]]
    ++
    [trigger_args_of_intercept:config(
         BitC_CHeaders, [], [],
         true, true, false,
         BitC_TStruct, "", BitC_TNewInstanceArgs, BitC_TNewInstanceBody,
         InterceptName, BitC_TBody,
         InterceptName, "bits") || InterceptName <- ["flock"]]
    ++
    [trigger_args_of_intercept:config(
         BitC_CHeaders, [], [],
         true, true, false,
         BitC_TStruct, "", BitC_TNewInstanceArgs, BitC_TNewInstanceBody,
         InterceptName, BitC_TBody,
         InterceptName, "bits") || InterceptName <- []]
    ++
    [trigger_random:config()]
    ++
    [
     %% A quick glance at the code suggests that the LevelDB code never
     %% checks the return status of a variable.  {shrug}

     #fi{	% both?/OS X version
         name = "access",
         type = intercept,
         intercept_args = "const char *path, int amode",
         intercept_args_call = "path, amode",
         c_headers = ["<unistd.h>"],
         intercept_errno = "ELOOP",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_access_path", "\"-unused-arg-\""},
                               {"random", "always", "100"}]
     },
     #fi{	% both?/OS X version
         name = "flock",
         type = intercept,
         intercept_args = "int fd, int checked_operation",
         intercept_args_call = "fd, checked_operation",
         c_headers = ["<sys/file.h>"],
         intercept_errno = "EBADF",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"random", "always", "100"},
                               {"i_arg_flock_bits", "flock_op_array"},
                               {"random", "", "80"}]
     },
     #fi{	% both?/OS X version
         name = "open",
         type = intercept,
         intercept_args = "const char *path, int checked_operation, ...",
         intercept_args_call = "path, checked_operation, mode",
         intercept_body_setup = "    int mode; va_list ap;\n\n" ++
                                "    va_start(ap, checked_operation); " ++
                                "mode = va_arg(ap, int); va_end(ap);\n",
         c_headers = ["<fcntl.h>", "<stdarg.h>"],
         intercept_errno = "EDQUOT",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"random", "always", "100"},
                               {"i_arg_open_path", "\"-unused-arg-\""},
                               {"random", "", "50"}]
     },
     %% #fi{	% both?/OS X version
     %%     name = "open",
     %%     type = intercept,
     %%     intercept_args = "const char *path, int checked_operation, ...",
     %%     intercept_args_call = "path, checked_operation",
     %%     c_headers = ["<fcntl.h>"],
     %%     intercept_errno = "ENOSPC",
     %%     intercept_return_type = "int",
     %%     intercept_return_value = "-1",
     %%     %% Use 2-tuple version here, have the instance name auto-generated
     %%     intercept_triggers = [{"random", "always", "100"},
     %%                           {"i_arg_open_bits", "open_write_op_array"},
     %%                           {"random", "", "50"}]
     %% },
     #fi{	% OS X version
         name = "unlink",
         type = intercept,
         intercept_args = "const char *path",
         intercept_args_call = "path",
         c_headers = ["<unistd.h>"],
         intercept_errno = "ENOSPC",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_unlink_path", "\"-unused-arg-\""},
                               {"random", "always", "100"}]
     },
     #fi{
         name = "unlinkat",	% Linux 3.2 version
         type = intercept,
         intercept_args = "int __fd, __const char *path, int __flag",
         intercept_args_call = "__fd, path, __flag",
         c_headers = ["<unistd.h>"],
         intercept_errno = "ENOSPC",
         intercept_return_type = "int",
         intercept_return_value = "-1",
         %% Use 2-tuple version here, have the instance name auto-generated
         intercept_triggers = [{"i_arg_unlinkat_path", "\"-unused-arg-\""},
                               {"random", "always", "100"}]
     }
    ].
