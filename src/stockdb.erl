%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).

-export([open/2, append/2, close/1]).
-export([test/0]).

%-spec open(Path::nonempty_string()) -> {ok, pid()} | {error, Reason::term()}.
open(Path, Modes) ->
  gen_server:start_link(stockdb_instance, {Path, Modes}, []).

append(Pid, Object) ->
  gen_server:call(Pid, {append, Object}).

close(Pid) ->
  gen_server:call(Pid, stop).


test() ->
  leb128:test(),
  stockdb_format:test(),
  stockdb_raw:test().
  
