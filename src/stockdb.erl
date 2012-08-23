%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).
-include("../include/stockdb.hrl").
-include("log.hrl").
-include("stockdb.hrl").

-type stockdb() :: {stockdb_pid, pid()} | term().

-type price() :: float().
-type volume() :: non_neg_integer().
-type quote() :: {price(), volume()}.
-type timestamp() :: non_neg_integer().
-type stock() :: atom().
-type date() :: string().

-type market_data() :: #md{}.
-type trade() :: #trade{}.


-export_type([stockdb/0, price/0, volume/0, quote/0, timestamp/0, stock/0, date/0]).
-export_type([market_data/0, trade/0]).


%% Application configuration
-export([get_value/1, get_value/2]).

%% Querying available data
-export([stocks/0, stocks/1, dates/1, dates/2, common_dates/1, common_dates/2]).
%% Get information about stock/date file
-export([info/1, info/2, info/3]).

%% Writing DB
-export([open_append/3, append/2, close/1]).
%% Reading existing data
-export([open_read/2, events/1, events/2, events/3]).
%% Iterator API
-export([init_reader/2, init_reader/3, read_event/1]).

%% Shortcut helpers
-export([candle/2, candle/3]).

%% Run tests
-export([run_tests/0]).


%% @doc List of stocks in local database
-spec stocks() -> [stock()].
stocks() -> stockdb_fs:stocks().

%% @doc List of stocks in remote database
-spec stocks(Storage::term()) -> [stock()].
stocks(Storage) -> stockdb_fs:stocks(Storage).


%% @doc List of available dates for stock
-spec dates(stock()|{any(),stock()}) -> [date()].
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
-spec open_read(stock()|{any(),stock()}, date()) -> {ok, stockdb()} | {error, Reason::term()}.  
open_read(Stock, Date) ->
  stockdb_reader:open(stockdb_fs:path(Stock, Date)).

%% @doc Open stock for appending
-spec open_append(stock(), date(), [open_option()]) -> {ok, stockdb()} | {error, Reason::term()}.  
open_append(Stock, Date, Opts) ->
  Path = stockdb_fs:path(Stock, Date),
  {db, RealStock, RealDate} = stockdb_fs:file_info(Path),
  stockdb_appender:open(Path, [{stock,RealStock},{date,stockdb_fs:parse_date(RealDate)}|Opts]).

%% @doc Append row to db
-spec append(stockdb(), trade() | market_data()) -> {ok, stockdb()} | {error, Reason::term()}.
append(Event, Stockdb) ->
  stockdb_appender:append(Event, Stockdb).


%% @doc Fetch information from opened stockdb
-spec info(stockdb()) -> [{Key::atom(), Value::term()}].
info(Stockdb) ->
  stockdb_reader:file_info(Stockdb).

%% @doc Fetch typical information about given Stock/Date
-spec info(stock(), date()) -> [{Key::atom(), Value::term()}].
info(Stock, Date) ->
  stockdb_reader:file_info(stockdb_fs:path(Stock, Date)).

%% @doc Fetch requested information about given Stock/Date
-spec info(stock(), date(), [Key::atom()]) -> [{Key::atom(), Value::term()}].
info(Stock, Date, Fields) ->
  stockdb_reader:file_info(stockdb_fs:path(Stock, Date), Fields).

%% @doc Get all events from filtered stock/date
-spec events(stock(), date(), [term()]) -> list(trade() | market_data()).
events(Stock, Date, Filters) ->
  case init_reader(Stock, Date, Filters) of
    {ok, Iterator} ->
      events(Iterator);
    {error, nofile} ->
      []
  end.

%% @doc Read all events for stock and date
-spec events(stock(), date()) -> list(trade() | market_data()).
events(Stock, Date) ->
  events(Stock, Date, []).

%% @doc Just read all events from stockdb
-spec events(stockdb()|iterator()) -> list(trade() | market_data()).
events(#dbstate{} = Stockdb) ->
  {ok, Iterator} = init_reader(Stockdb, []),
  events(Iterator);

events(Iterator) ->
  stockdb_iterator:all_events(Iterator).

%% @doc Init iterator over opened stockdb
% Options: 
%    {range, Start, End}
%    {filter, FilterFun, FilterArgs}
% FilterFun is function in stockdb_filters
-spec init_reader(stockdb(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(#dbstate{} = Stockdb, Filters) ->
  case stockdb_iterator:init(Stockdb) of
    {ok, Iterator} ->
      init_reader(Iterator, Filters);
    {error, _} = Error ->
      Error
  end;

init_reader(Iterator, Filters) ->
  {ok, apply_filters(Iterator, Filters)}.

%% @doc Shortcut for opening iterator on stock-date pair
-spec init_reader(stock(), date(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(Stock, Date, Filters) ->
  case open_read(Stock, Date) of
    {ok, Stockdb} ->
      init_reader(Stockdb, Filters);
    {error, _} = Error ->
      Error
  end.


apply_filter(Iterator, false) -> Iterator;
apply_filter(Iterator, {range, Start, End}) ->
  stockdb_iterator:set_range({Start, End}, Iterator);
apply_filter(Iterator, {filter, Function, Args}) ->
  stockdb_iterator:filter(Iterator, Function, Args).

apply_filters(Iterator, []) -> Iterator;
apply_filters(Iterator, [Filter|MoreFilters]) ->
  apply_filters(apply_filter(Iterator, Filter), MoreFilters).


%% @doc Read next event from iterator
-spec read_event(iterator()) -> {ok, trade() | market_data(), iterator()} | {eof, iterator()}.
read_event(Iterator) ->
  stockdb_iterator:read_event(Iterator).

%% @doc close stockdb
-spec close(stockdb()) -> ok.
close(#dbstate{file = F} = _Stockdb) ->
  case F of 
    undefined -> ok;
    _ -> file:close(F)
  end,
  ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec candle(stock(), date()) -> {price(),price(),price(),price()}.
candle(Stock, Date) ->
  candle(Stock, Date, []).


-spec candle(stock(), date(), list(reader_option())) -> {price(),price(),price(),price()}.
candle(Stock, Date, Options) ->
  stockdb_helpers:candle(Stock, Date, Options).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Configuration
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
%% @doc get configuration value with fallback to given default
get_value(Key, Default) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined -> Default
  end.

%% @private
%% @doc get configuration value, raise error if not found
get_value(Key) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined -> erlang:error({no_key,Key})
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
run_tests() ->
  eunit:test({application, stockdb}).

