%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).

-export([read/1, open/1, write/2, close/1]).
-export([test/0]).

%-spec open(Path::nonempty_string()) -> {ok, pid()} | {error, Reason::term()}.
open(Path) ->
  {error, not_implemented}.

read(_) -> {error, not_implemented}.
write(_,_) -> {error, not_implemented}.
close(_) -> {error, not_implemented}.

test() ->
  leb128:test().
