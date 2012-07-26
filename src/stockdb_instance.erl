-module(stockdb_instance).
-author({"Danil Zagoskin", z@gosk.in}).

-export([init/1, handle_call/3, terminate/2, code_change/3]).


init({File, Options}) ->
  {ok, _State} = stockdb_raw:open(File, [raw|Options]).

handle_call({append, Object}, _From, State) ->
  {ok, NewState} = stockdb_raw:append(Object, State),
  {reply, ok, NewState};

handle_call(stop, _From, State) ->
  {stop, normal, ok, State}.

code_change(_, State, _) ->
  {ok, State}.

terminate(_Reason, State) ->
  stockdb_raw:close(State).
