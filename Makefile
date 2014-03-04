REBAR ?= ./rebar

all: compile

get-deps:
	./c_src/build_deps.sh get-deps

deps:
	${REBAR} get-deps

rm-deps:
	./c_src/build_deps.sh rm-deps

compile: deps
	${REBAR} compile

test: compile_scenarios

compile_scenarios:
	-mkdir -p .eunit
	erlc -o deps/faulterl/ebin -I deps/faulterl/include priv/scenario/*erl

clean:
	${REBAR} clean

include tools.mk
