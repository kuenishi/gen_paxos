.PHONY: test all compile


all: compile

compile:
	./rebar3 compile

test:
	./rebar3 eunit --dir=test,src
