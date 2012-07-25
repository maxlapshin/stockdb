%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).

-export([open/2, append/2, close/1]).
-export([autocreate_table/0]).
-export([test/0]).

%-spec open(Path::nonempty_string()) -> {ok, pid()} | {error, Reason::term()}.
open(Path, Modes) ->
  case table_exists() of
    false ->
      {error, app_not_started};
    true ->
      Ref = erlang:make_ref(),
      {ok, State} = stockdb_raw:open(Path, Modes),
      ets:insert(stockdb_instances, {Ref, Path, State}),
      {ok, Ref}
  end.

append(Ref, Object) ->
  State = ets:lookup_element(stockdb_instances, Ref, 3),
  {ok, NewState} = stockdb_raw:append(Object, State),
  true = ets:update_element(stockdb_instances, Ref, {3, NewState}),
  ok.

close(Ref) ->
  State = ets:lookup_element(stockdb_instances, Ref, 3),
  ets:delete(stockdb_instances, Ref),
  ok = stockdb_raw:close(State).

table_exists() ->
  ets:info(stockdb_instances, name) == stockdb_instances.

autocreate_table() ->
  case ets:info(stockdb_instances, name) of
    stockdb_instances ->
      ok;
    undefined ->
      % Need to create table
      ets:new(stockdb_instances, [set, public, named_table, {write_concurrency,true}, {read_concurrency, true}])
  end.

test() ->
  leb128:test(),
  stockdb_format:test(),
  stockdb_raw:test().
  
