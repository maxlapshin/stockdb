
all:
	ERL_LIBS=apps:deps erl -make

app:
	@./rebar compile

clean:
	@./rebar clean
	@rm -f erl_crash.dump

test:
	@./rebar compile eunit


PLT_NAME=.stockdb_dialyzer.plt

$(PLT_NAME):
	@ERL_LIBS=deps dialyzer --build_plt --output_plt $@ \
		--apps kernel stdlib sasl crypto || true

dialyze: $(PLT_NAME)
	@dialyzer ebin  --plt $(PLT_NAME) --no_native \
		-Werror_handling -Wunderspecs
