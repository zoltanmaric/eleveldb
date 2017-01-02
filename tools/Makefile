# -------------------------------------------------------------------
#
# Copyright (c) 2011-2017 Basho Technologies, Inc.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------------
#
# Requires GNU Make functionality and a Unix-y environment.
#

prj_dir	:= $(abspath $(CURDIR)/..)
project	:= $(strip $(notdir $(prj_dir)))

module	:= eleveldb
version	:= $(shell git describe --tags 2>/dev/null | cut -d- -f1)

cache_dir := $(prj_dir)/.cache
e_gen_dir := $(prj_dir)/gen
tools_dir := $(abspath $(CURDIR))
caps_dir  := $(tools_dir)/capabilities

# there are multiple places we may find dependencies in Rebar3
# we may also be running make outside of rebar, so test for each variable
# the resulting list *may* contain duplicates, but it's not worth de-duping
deps_dirs :=
ifneq ($(REBAR_CHECKOUTS_DIR),)
deps_dirs := $(wildcard $(REBAR_CHECKOUTS_DIR))
endif
ifneq ($(REBAR_BUILD_DIR),)
deps_dirs += $(wildcard $(REBAR_BUILD_DIR)/lib)
endif
ifneq ($(REBAR_DEPS_DIR),)
deps_dirs += $(wildcard $(REBAR_DEPS_DIR))
endif
ifneq ($(REBAR_ROOT_DIR),)
deps_dirs += $(wildcard $(REBAR_ROOT_DIR)/..)
endif
deps_dirs += $(wildcard $(prj_dir)/..)
deps_dirs := $(abspath $(deps_dirs))

db_root	:= $(firstword $(wildcard $(patsubst %,%/leveldb,$(deps_dirs))))
sn_root	:= $(firstword $(wildcard $(patsubst %,%/snappy,$(deps_dirs))))
ee_root	:= $(patsubst %/,%,$(firstword \
		$(dir $(wildcard $(patsubst %,%/leveldb_ee/*.cc,$(deps_dirs)))) \
		$(wildcard $(db_root)/leveldb_os $(db_root)/stubs) ))

# probably overkill, but we really want the system's POSIX uname and not
# any wrapper/alternate that may come first in the path
uname	:= $(or $(wildcard /usr/bin/uname), \
		$(wildcard /bin/uname), \
		$(shell which uname 2>/dev/null), \
		$(error Can't find acceptable uname program))

# match the 'Osname' returned by erlang os:type()
sys_type := $(shell $(uname) -s | tr '[A-Z]' '[a-z]')
# it's unlikely there's any case variation, but make sure
sys_arch := $(shell $(uname) -m | tr '[A-Z]' '[a-z]')
sys_rel  := $(shell $(uname) -r | tr '[A-Z]' '[a-z]')

#
# All about erlang ...
#
# This will normally be invoked by rebar3, so use the environment variables
# it sets if they're present rather than interogating the Erlang runtime.
# The variables rebar sets are not fully documented, so be cautious.
#
erl_exe	:= $(or $(if $(ERL),$(shell which $(ERL) 2>/dev/null)), \
		$(shell which erl 2>/dev/null), \
		$(error Can't find Erlang/OTP runtime))
erl_eval := $(erl_exe) -noshell -eval

#
# It's too much of a nuisance to filter the major release out of
# $(ERLANG_TARGET) (which really isn't formally documented anyway),
# where it could be either the R... or NN... form, so just get it
# from erl.
#
erl_tmp	:= S = case erlang:system_info(otp_release) of
erl_tmp	+= [$$R|RelR] -> RelR; Rel -> Rel end,
erl_tmp	+= {R,_} = string:to_integer(S),
erl_tmp	+= io:put_chars(erlang:integer_to_list(R)), erlang:halt().
otp_rel	:= $(shell $(erl_eval) '$(erl_tmp)' 2>/dev/null)

# 'nif_version' added in OTP-17, punt before that - R16's NIF API is v2.4
erl_tmp	:= S = case erlang:system_info(otp_release) of
erl_tmp	+= [$$R|_] -> "2.4"; _ -> erlang:system_info(nif_version) end,
erl_tmp	+= io:put_chars(S), erlang:halt().
nif_api := $(shell $(erl_eval) '$(erl_tmp)' 2>/dev/null)

ifneq ($(ERLANG_ERTS_VER),)
erts_vsn := $(ERLANG_ERTS_VER)
else
erl_tmp	:= io:put_chars(erlang:system_info(version)), erlang:halt().
erts_vsn := $(shell $(erl_eval) '$(erl_tmp)' 2>/dev/null)
endif

ifneq ($(ERLANG_ROOT_DIR),)
erts_inc := $(ERLANG_ROOT_DIR)/erts-$(erts_vsn)/include
else
erl_tmp	:= S = [code:root_dir(), "/erts-", "$(erts_vsn)", "/include"],
erl_tmp	+= io:put_chars(S), erlang:halt().
erts_inc := $(shell $(erl_eval) '$(erl_tmp)' 2>/dev/null)
endif

ifneq ($(ERLANG_LIB_DIR_erl_interface),)
ei_inc	:= $(ERLANG_LIB_DIR_erl_interface)/include
ei_lib	:= $(ERLANG_LIB_DIR_erl_interface)/lib
else
erl_tmp	:= io:put_chars(code:lib_dir(erl_interface)), erlang:halt().
erl_tmp	:= $(shell $(erl_eval) '$(erl_tmp)' 2>/dev/null)
ei_inc	:= $(erl_tmp)/include
ei_lib	:= $(erl_tmp)/lib
endif

# base name of the NIF library that will be calculated by the loader
so_name	:= $(module)_$(sys_type)_$(sys_arch)_otp$(otp_rel)
# where it's going to end up when we're done, this is the default target
so_dest	:= $(prj_dir)/priv/$(so_name).so

db_tool_names := leveldb_repair perf_dump sst_rewrite sst_scan
db_tools := $(patsubst %,$(prj_dir)/priv/%_$(sys_type)_$(sys_arch),$(db_tool_names))

# directories where intermediate artifacts will go
work_dir    := $(prj_dir)/_work/otp-$(otp_rel)/$(sys_type)-$(sys_arch)-$(sys_rel)
feat_dir    := $(work_dir)/feat
c_gen_dir   := $(work_dir)/gen_c
d_gen_dir   := $(work_dir)/gen_d
ar_obj_dir  := $(work_dir)/ar
so_obj_dir  := $(work_dir)/so
stage_dir   := $(work_dir)/stage

# intermediate static archive
ar_name	:= static_$(sys_type)_$(sys_arch)
ar_dest	:= $(ar_obj_dir)/lib$(ar_name).a

#
# Leveldb has a legthy Makefile and build_detect_platform script.
# We can figure out the same things they do in here in less space by
# leveraging what we already know about environment constraints - for
# instance, we know we're building on a POSIX platform with snappy, and
# whether leveldb_ee is installed.
#
db_conf	:= $(c_gen_dir)/leveldb/ldb_config.h
db_defs	:= LEVELDB_PLATFORM_POSIX SNAPPY
db_vsn	:= $(shell cd $(db_root) && git describe --tags 2>/dev/null | cut -d- -f1)
db_srcs	:= $(filter-out %_test.c %_test.cc %/db/db_bench.cc \
		%/util/testharness.cc %/util/testutil.cc, \
		$(wildcard $(db_root)/db/*.cc) \
		$(wildcard $(db_root)/table/*.cc) \
		$(wildcard $(db_root)/util/*.c $(db_root)/util/*.cc) \
		$(wildcard $(ee_root)/*.cc) )
db_srcs	+= $(db_root)/port/port_posix.cc
db_dirs	:= $(patsubst %/,%,$(sort $(dir $(db_srcs))))

# do this in stages for sanity
db_deps	:= $(patsubst $(db_root)/%.cc,$(d_gen_dir)/$(notdir $(db_root))/%.d,$(db_srcs))
db_deps	:= $(patsubst $(db_root)/%.c,$(d_gen_dir)/$(notdir $(db_root))/%.d,$(db_deps))
# if $(ee_root) is under $(db_root), this will already have been filtered out
db_deps	:= $(patsubst $(ee_root)/%.cc,$(d_gen_dir)/$(notdir $(ee_root))/%.d,$(db_deps))
# db_deps	:= $(patsubst $(db_root)/%.c,$(d_gen_dir)/$(notdir $(db_root))/%.d, \
# 	$(patsubst $(db_root)/%.cc,$(d_gen_dir)/$(notdir $(db_root))/%.d, \
# 	$(patsubst $(ee_root)/%.cc,$(d_gen_dir)/$(notdir $(ee_root))/%.d, \
# 	$(db_srcs) )))
# leveldb tools are handled separately
db_deps	+= $(patsubst %,$(d_gen_dir)/$(notdir $(db_root))/tools/%.d,$(db_tool_names))

#
# Snappy, OTOH, wants to use autoconf, but we can do the same thing in a much
# simpler script to generate the config headers.
#
sn_conf	:= $(c_gen_dir)/snappy/config.h
sn_stub	:= $(c_gen_dir)/snappy-stubs-public.h
sn_vsn	:= $(shell cd $(sn_root) && git describe --tags 2>/dev/null | cut -d- -f1)
sn_srcs	:= $(filter-out %test.cc, $(wildcard $(sn_root)/*.cc))
sn_deps	:= $(patsubst $(dir $(sn_root))%.cc,$(d_gen_dir)/%.d,$(sn_srcs))

#
# The NIF is pretty straightforward
#
nif_srcs := $(wildcard $(prj_dir)/c_src/*.cc)
nif_deps := $(foreach n, $(sort $(basename $(notdir \
		$(nif_srcs)))), $(d_gen_dir)/$(project)/$(n).d)


o_level	:= -g2 -O3

db_libs	:= -L$(ar_obj_dir) -l$(ar_name)
so_libs	:= $(db_libs) -L$(ei_lib) -lerl_interface -lei
os_libs	:=

ifeq ($(DEBUG),)
CPPFLAGS := -DNDEBUG
else
CPPFLAGS :=
endif

LDFLAGS := -m64
CFLAGS	:= $(LDFLAGS)
CXXFLAGS := $(LDFLAGS)
so_flag	:= -shared
ecp	:= /bin/cp -p
esed	:= /usr/bin/sed -E
# build tools
ifeq	($(sys_type),darwin)
db_defs += OS_MACOSX
ecp	:= /bin/cp -pX
CC	:= /usr/bin/cc
CXX	:= /usr/bin/c++
LDFLAGS := -arch x86_64
LDFLAGS	+= $(shell test $(firstword $(subst ., ,$(sys_rel))) -lt 14 \
		|| echo '-mmacosx-version-min=10.9')
CFLAGS	:= $(LDFLAGS)
CXXFLAGS := $(LDFLAGS)
so_flag	:= -flat_namespace -undefined suppress -dynamiclib
else ifeq ($(sys_type),freebsd)
db_defs += _REENTRANT OS_FREEBSD
CC	:= $(shell which cc)
CXX	:= $(shell which c++)
os_libs	+= -lpthread
else ifeq ($(sys_type),linux)
db_defs += OS_LINUX
esed	:= /usr/bin/sed -r
CC	:= $(shell which gcc)
CXX	:= $(shell which g++)
os_libs	+= -pthread -lrt
else ifeq ($(sys_type),sunos)
db_defs += _REENTRANT OS_SOLARIS
ecp	:= /usr/bin/cp -p
os_libs	+= -lpthread -lrt
undefine esed
CC	:= $(shell which gcc)
CXX	:= $(shell which g++)
else
$(error Unsupported platform '$(sys_type)')
endif
CFLAGS    += -std=c99 $(o_level) -fPIC -Wall -Wmissing-prototypes
CXXFLAGS  += $(o_level) -fPIC -Wall
LDFLAGS   += $(o_level)

db_libs	+= $(os_libs)
so_libs	+= $(os_libs)

#
# test some platform/compiler features
#
ifneq ($(shell $(tools_dir)/test_run.sh $(feat_dir)/cc_is_clang \
	$(CXX) -o $(feat_dir)/cc_is_clang $(caps_dir)/is_clang.cc),pass)
CFLAGS	  += -fno-builtin-memcmp
CXXFLAGS  += -fno-builtin-memcmp
endif
ifeq ($(shell $(tools_dir)/test_build.sh $(feat_dir) \
	$(CXX) -std=c++0x -c -o $(feat_dir)/cstdatomic.o $(caps_dir)/cstdatomic.cc),pass)
CXXFLAGS  += -std=c++0x
db_defs   += LEVELDB_CSTDATOMIC_PRESENT
endif
ifeq ($(shell $(tools_dir)/test_build.sh $(feat_dir) \
	$(CXX) -ltcmalloc -o $(feat_dir)/cc_main $(caps_dir)/main.cc),pass)
LDFLAGS	+= -ltcmalloc
endif


V	?= 0
ifeq	($(V),0)
c_verbose   = @echo ' C      ' $(<F);
cc_verbose  = @echo ' C++    ' $(<F);
d_verbose   = @echo ' DEPS   ' $(<F);
ar_verbose  = @echo ' AR     ' $(@F);
ld_verbose  = @echo ' LD     ' $(@F);
cp_verbose  = @echo ' CP     ' $(@F);
endif
compile_c   = $(CC) -c $(CFLAGS) $(CPPFLAGS)
compile_cc  = $(CXX) -c $(CXXFLAGS) $(CPPFLAGS)
deps_c      = $(CC) -MM -E $(CFLAGS) $(CPPFLAGS)
deps_cc     = $(CXX) -MM -E $(CXXFLAGS) $(CPPFLAGS)
archive     = $(AR)
link        = $(CXX) $(LDFLAGS)

db_cppflags   := $(foreach d, $(db_defs), -D$(d))
db_cppflags   += '-DLEVELDB_VSN=$(db_vsn)'
db_cppflags   += -I$(c_gen_dir) -I$(db_root) -I$(db_root)/include
db_cppflags   += -I$(c_gen_dir)/snappy -I$(sn_root) -I$(abspath $(sn_root)/..)
nif_cppflags  := -I$(c_gen_dir) -I$(db_root)/include -I$(db_root)
nif_cppflags  += -I$(sn_root) -I$(erts_inc) -I$(ei_inc)
sn_cppflags   := -I$(c_gen_dir)/snappy -I$(c_gen_dir)

so_objs	:= $(foreach n, $(sort $(basename $(notdir \
		$(nif_srcs)))), $(so_obj_dir)/$(n).o)

ar_objs	:= $(foreach n, $(sort $(basename $(notdir \
		$(sn_srcs) $(db_srcs)))), $(ar_obj_dir)/$(n).o)

vpath %.cc $(db_dirs)
vpath %.c $(db_dirs)

.PHONY:	default dist clean clean-dist veryclean build_info

# don't automatically clean up intermediates
.SECONDARY:

default :: dist

dist :: $(so_dest) $(db_tools)

clean ::
	/bin/rm -rf $(work_dir)

clean-dist :: clean
	/bin/rm -f $(so_dest) $(db_tools)

veryclean :: clean-dist

build_info ::
	@echo 'OS Name:        $(sys_type)'
	@echo 'OS Arch:        $(sys_arch)'
	@echo 'OS Release      $(sys_rel)'
	@echo 'OS Major:       $(firstword $(subst ., ,$(sys_rel)))'
	@echo 'OTP Release:    $(otp_rel)'
	@echo 'ERTS Version:   $(erts_vsn)'
	@echo 'NIF API:        $(nif_api)'
	@echo 'ERTS Include:   $(erts_inc)'
	@echo 'EI Include:     $(ei_inc)'
	@echo 'EI Libs:        $(ei_lib)'
	@echo 'NIF Library:    $(so_dest)'
	@echo 'Deps Dirs:      $(deps_dirs)'

$(prj_dir)/priv/% : $(stage_dir)/%
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(cp_verbose) $(ecp) $< $@

$(stage_dir)/$(notdir $(so_dest)) : $(ar_dest) $(so_objs)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(ld_verbose) $(strip $(link) $(so_flag) $(so_libs) -o $@ $(so_objs))

$(stage_dir)/%_$(sys_type)_$(sys_arch) : $(db_root)/tools/%.cc $(ar_dest)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(ld_verbose) $(strip $(CXX) \
		$(CXXFLAGS) $(CPPFLAGS) $(db_cppflags) $(db_libs) -o $@ $<)

$(ar_dest) : $(ar_objs)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(ar_verbose) $(strip $(archive) -rcs $@ $(ar_objs))

$(so_obj_dir)/%.o : $(c_gen_dir)/%.cc
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(cc_verbose) $(strip $(compile_cc) $(nif_cppflags) -o $@ $<)

$(so_obj_dir)/%.o : $(prj_dir)/c_src/%.cc
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(cc_verbose) $(strip $(compile_cc) $(nif_cppflags) -o $@ $<)

$(ar_obj_dir)/%.o : $(sn_root)/%.cc $(sn_conf)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(cc_verbose) $(strip $(compile_cc) $(sn_cppflags) -o $@ $<)

# these are files found on the vpath, which will be leveldb only
$(ar_obj_dir)/%.o : %.cc $(sn_stub) $(db_conf)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(cc_verbose) $(strip $(compile_cc) $(db_cppflags) -o $@ $<)

$(ar_obj_dir)/%.o : %.c $(sn_stub) $(db_conf)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(c_verbose) $(strip $(compile_c) $(db_cppflags) -o $@ $<)

# leveldb compile configuration
$(db_conf) : Makefile
	@test -d $(@D) || /bin/mkdir -p $(@D)
	@echo Generating leveldb config headers ...
	@printf '/*\n * This is a generated file - DO NOT EDIT!\n */\n\n' > $@
	@$(foreach d, $(sort $(db_defs)), \
		printf '#ifndef %s\n#define %s\n#endif\n\n' $(d) $(d) >> $@ ;)

# snappy compile configuration
# intermediate dependency to avoid building more than once
$(sn_conf) $(sn_stub) : $(feat_dir)/snappy.config.gen

$(feat_dir)/snappy.config.gen : \
		Makefile $(tools_dir)/snappy_conf.sh \
		$(wildcard $(sn_root)/*.c* $(sn_root)/*.h*)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	@echo Generating snappy config headers ...
	@$(tools_dir)/snappy_conf.sh '$(sn_conf)' '$(sn_stub)' \
		'$(sn_root)' '$(caps_dir)' '$(feat_dir)' '$(CXX)'
	@touch $@

#
# dependency files
#

$(d_gen_dir)/$(project)/%.d : $(c_gen_dir)/%.cc $(db_conf) $(sn_stub)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(d_verbose) $(strip $(deps_cc) $(nif_cppflags) \
		-MT $@ -MT $(so_obj_dir)/$(basename $(@F)).o -MF $@ $<)

$(d_gen_dir)/$(project)/%.d : $(prj_dir)/c_src/%.cc $(db_conf) $(sn_stub)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(d_verbose) $(strip $(deps_cc) $(nif_cppflags) \
		-MT $@ -MT $(so_obj_dir)/$(basename $(@F)).o -MF $@ $<)

$(d_gen_dir)/$(notdir $(db_root))/tools/%.d : \
		$(db_root)/tools/%.cc $(db_conf) $(sn_stub)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(d_verbose) $(strip $(deps_cc) $(db_cppflags) -MT $@ -MT \
		$(stage_dir)/$(basename $(@F))_$(sys_type)_$(sys_arch) \
		-MF $@ $<)

$(d_gen_dir)/$(notdir $(db_root))/%.d : $(db_root)/%.cc $(db_conf) $(sn_stub)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(d_verbose) $(strip $(deps_cc) $(db_cppflags) \
		-MT $@ -MT $(ar_obj_dir)/$(basename $(@F)).o -MF $@ $<)

$(d_gen_dir)/$(notdir $(db_root))/%.d : $(db_root)/%.c $(db_conf) $(sn_stub)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(d_verbose) $(strip $(deps_c) $(db_cppflags) \
		-MT $@ -MT $(ar_obj_dir)/$(basename $(@F)).o -MF $@ $<)

$(d_gen_dir)/$(notdir $(sn_root))/%.d : $(sn_root)/%.cc $(sn_conf)
	@test -d $(@D) || /bin/mkdir -p $(@D)
	$(d_verbose) $(strip $(deps_cc) $(sn_cppflags) \
		-MT $@ -MT $(ar_obj_dir)/$(basename $(@F)).o -MF $@ $<)


# de-dup in case any weirdness got through
-include $(sort $(nif_deps) $(db_deps) $(sn_deps))
