
all:
	ERL_LIBS=apps:deps erl -make

app:
	@./rebar compile

clean:
	@./rebar clean
	@rm -f erl_crash.dump

test:
	@./rebar compile eunit

