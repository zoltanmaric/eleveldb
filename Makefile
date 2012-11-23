TARGET=		eleveldb

REBAR=		./rebar
#REBAR=		/usr/bin/env rebar
ERL=		/usr/bin/env erl
DIALYZER=	/usr/bin/env dialyzer


.PHONY: plt analyze all deps compile get-deps clean

all: compile

deps: get-deps

get-deps:
	@$(REBAR) get-deps

update-deps:
	@$(REBAR) update-deps

c_src/eleveldb.o: c_src/async_nif.h
	touch c_src/eleveldb.cc

compile: c_src/eleveldb.o
	@$(REBAR) compile

clean:
	@$(REBAR) clean

test: eunit

eunit: compile
	@$(REBAR) eunit skip_deps=true

eunit_console:
	@$(ERL) -pa .eunit deps/*/ebin

plt: compile
	@$(DIALYZER) --build_plt --output_plt .$(TARGET).plt -pa deps/*/ebin --apps kernel stdlib

analyze: compile
	$(DIALYZER) --plt .$(TARGET).plt -pa deps/*/ebin ebin

repl:
	$(ERL) -pz deps/*/ebin -pa ebin

gdb-repl:
	USE_GDB=1 $(ERL) -pz deps/*/ebin -pa ebin


# NOTES
#
# When working on async_nif.h there are two thigns to remember:
#  1. clang++ provides better error messages
#  2. `rebar compile` doesn't know that eleveldb.cc depends on async_nif.h
#     and so it doesn't recompile that file.
#
# My (greg@basho.com) workaround is to build using this command.
# touch c_src/eleveldb.cc && CXX=clang++ ./rebar compile
