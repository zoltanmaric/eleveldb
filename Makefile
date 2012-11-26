
all: deps compile

deps:
	./rebar get-deps

compile:
	./rebar compile

test: compile
	./rebar eunit

clean:
	./rebar clean
