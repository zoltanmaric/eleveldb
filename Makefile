# -------------------------------------------------------------------
#
# Copyright (c) 2011-2016 Basho Technologies, Inc.
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

prj_dir	:= $(CURDIR)
cache	:= $(prj_dir)/.cache

dl_tgts	:=
#
# tools
#
REBAR3	?= $(cache)/rebar3
ifneq ($(wildcard $(REBAR3)),)
rebar	:= $(REBAR3)
else
rebar	:= $(cache)/rebar3
dl_tgts	+= $(rebar)
endif

#
# how to download files if we need to
#
ifneq ($(dl_tgts),)
dlcmd	:= $(shell which wget 2>/dev/null || true)
ifneq ($(wildcard $(dlcmd)),)
dlcmd	+= -O
else
dlcmd	:= $(shell which curl 2>/dev/null || true)
ifneq ($(wildcard $(dlcmd)),)
dlcmd	+= -o
else
$(error Need wget or curl to download files)
endif
endif
endif

.PHONY	: check clean clean-deps clean-docs compile \
	  default docs prereqs test veryclean

default : compile

compile : prereqs
	$(rebar) as prod compile

check : prereqs
	$(rebar) as check do dialyzer, xref

clean : prereqs
	$(rebar) as prod clean

clean-deps :: clean
	/bin/rm -rf $(prj_dir)/_build

clean-docs :
	/bin/rm -rf $(prj_dir)/doc/*

docs : prereqs
	$(rebar) edoc

test : prereqs
	$(rebar) eunit

veryclean :: clean clean-deps clean-dist clean-docs
	/bin/rm -rf $(cache)

#
# downloads
#
prereqs :: $(dl_tgts)

$(cache)/rebar3 :
	@test -d $(@D) || /bin/mkdir -p $(@D)
	@echo Downloading $@ ...
	@$(dlcmd) $@ https://s3.amazonaws.com/rebar3/rebar3
	@/bin/chmod +x $@

