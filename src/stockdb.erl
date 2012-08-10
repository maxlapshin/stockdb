%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).
-include("../include/stockdb.hrl").
-include("log.hrl").

%% Application configuration
-export([get_value/1, get_value/2]).

%% DB range processing
-export([foldl/4]).


-export([stocks/0, stocks/1, dates/1, dates/2, common_dates/1, common_dates/2]).
-export([open_read/2, open_append/3]).
-export([append/2]).
-export([chunks/1]).
-export([init_reader/2, read_event/1]).
-export([close/1]).

%% Run tests
-export([run_tests/0]).

-define(STOCK_DATE_DELIMITER, ":").



%%% Desired API


%% @doc List of stocks in local database
-spec stocks() -> [stock()].
stocks() -> stockdb_fs:stocks().

%% @doc List of stocks in remote database
-spec stocks(Storage::term()) -> [stock()].
stocks(Storage) -> stockdb_fs:stocks(Storage).


%% @doc List of available dates for stock
-spec dates(stock()) -> [date()].
dates(Stock) -> stockdb_fs:dates(Stock).

%% @doc List of available dates in remote database
-spec dates(Storage::term(), Stock::stock()) -> [date()].
dates(Storage, Stock) -> stockdb_fs:dates(Storage, Stock).

%% @doc List dates when all given stocks have data
-spec common_dates([stock()]) -> [date()].
common_dates(Stocks) -> stockdb_fs:common_dates(Stocks).

%% @doc List dates when all given stocks have data, remote version
-spec common_dates(Storage::term(), [stock()]) -> [date()].
common_dates(Storage, Stocks) -> stockdb_fs:common_dates(Storage, Stocks).


%% @doc Open stock for reading
-spec open_read(stock(), date()) -> {ok, stockdb()} | {error, Reason::term()}.  
open_read(Stock, Date) ->
  stockdb_reader:open(stockdb_fs:path(Stock, Date)).

%% @doc Open stock for appending
-spec open_append(stock(), date(), [open_option()]) -> {ok, stockdb()} | {error, Reason::term()}.  
open_append(Stock, Date, Opts) ->
  stockdb_appender:open(stockdb_fs:path({proplists:get_value(type,Opts,stock), Stock}, Date), [{stock,Stock},{date,stockdb_fs:parse_date(Date)}|Opts]).

%% @doc Append row to db
-spec append(stockdb(), trade() | market_data()) -> {ok, stockdb()} | {error, Reason::term()}.
append(Stockdb, Event) ->
  stockdb_appender:append(Stockdb, Event).

%% @doc List of chunks in file
-spec chunks(stockdb()) -> list(timestamp()).
chunks(_Stockdb) ->
  [].


%% @doc Init iterator over opened stockdb
-spec init_reader(stockdb(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(_Stockdb, _Opts) ->
  {error, not_implemented}.

%% @doc Read next event from iterator
-spec read_event(iterator()) -> {ok, trade() | market_data(), iterator()} | {eof, iterator()}.
read_event(Iterator) ->
  {eof, Iterator}.

%% @doc close stockdb
-spec close(stockdb()) -> ok.
close(_Stockdb) ->
  ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%        File API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Fold over DB entries
% Fun(Event, Acc1) -> Acc2
% Stock — 'MICEX.URKA'
% Options: 
%    {day, "2012/01/05"}
%    {range, Start, End}
%    {filter, FilterFun}
% FilterFun :: fun(Event, State) -> {[Event], NewState}.
-spec foldl(Filter, Acc0, stock(), list()) -> Acc1 when
  Filter :: fun((Elem :: market_data()|trade(), AccIn :: term()) -> AccOut :: term()),
  Acc0 :: term(),
  Acc1 :: term().
foldl(Fun, Acc0, Stock, Options) ->
  ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Configuration
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc get configuration value with fallback to given default
get_value(Key, Default) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined -> Default
  end.

%% @doc get configuration value, raise error if not found
get_value(Key) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined -> erlang:error({no_key,Key})
  end.






%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

run_tests() ->
  eunit:test({application, stockdb}).

