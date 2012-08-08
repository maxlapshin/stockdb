%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).

-export([open/2, append/2, close/1]).
-export([foldl/4, read_event/1]).
-export([test/0]).

-type stockdb() :: any().
-type price() :: float().
-type volume() :: non_neg_integer().
-type quotes() :: [{price(), volume()}].
-type timestamp() :: non_neg_integer().
-type market_data() :: {md, timestamp(), quotes(), quotes()}.
-type trade() :: {trade, timestamp(), price(), volume()}.
-type stock() :: atom().


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
  


% @doc
% Fun(Event, Acc1) -> Acc2
% Stock — 'MICEX.URKA'
% Options: 
%    {day, "2012/01/05"}
%    {range, Start, End}
%    {filter, candlestick}
-spec foldl(Filter, Acc0, stock(), list()) -> Acc1 when
  Filter :: fun((Elem :: market_data()|trade(), term()) -> term()),
  Acc0 :: term(),
  Acc1 :: term().
foldl(_Fun, _Acc0, _Stock, _Options) ->
  error(unimplemented).


% @doc
% Takes either pid, either raw stockdb, returns one event

-spec read_event(stockdb()) -> {ok, market_data() | trade(), stockdb()} | {eof, stockdb()}.
read_event(Stockdb) ->
  {eof, Stockdb}.
