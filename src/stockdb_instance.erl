-module(stockdb_instance).
-author({"Danil Zagoskin", z@gosk.in}).

-export([init/1, handle_call/3, terminate/2, code_change/3]).


init({File, Options}) ->
  {ok, _State} = stockdb_raw:open(File, [raw|Options]).

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};

handle_call(Function, _From, State) when is_atom(Function) ->
  {ok, NewState} = stockdb_raw:Function(State),
  {reply, ok, NewState};

handle_call({Function, Arg1}, _From, State) when is_atom(Function) ->
  {ok, NewState} = stockdb_raw:Function(Arg1, State),
  {reply, ok, NewState}.

code_change(_, State, _) ->
  {ok, State}.

terminate(_Reason, State) ->
  stockdb_raw:close(State).
