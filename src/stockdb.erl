%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).
-include("../include/stockdb.hrl").
-include("log.hrl").

%% Application configuration
-export([get_value/1, get_value/2, root/0]).

%% Filesystem querying
% -export([path/2, list_db/0, list_db/1, stocks/0, stock_dates/1, common_dates/1, file_info/1]).

%% DB range processing
-export([foldl/4]).


-export([stocks/0, stocks/1, dates/1, dates/2]).
-export([open_read/2, open_append/2]).
-export([append/2]).
-export([chunks/1]).
-export([init_reader/2, read_event/1]).
-export([close/1]).

%% Run tests
-export([run_tests/0]).

-define(DATE_DELIMITER, "-").
-define(STOCK_DATE_DELIMITER, ":").



%%% Desired API

%% @doc List of stocks in local database
-spec stocks() -> list(stock()).
stocks() ->
  [].

%% @doc List of stocks in remote database
-spec stocks(Storage::term()) -> list(stock()).
stocks(_Storage) ->
  [].


%% @doc List of available dates for stock
-spec dates(stock()) -> list(date()).
dates(_Stock) ->
  [].


%% @doc List of available dates in remote database
-spec dates(Storage::term(), Stock::stock()) -> list(date()).
dates(_Storage, _Stock) ->
  [].


%% @doc Open stock for reading
-spec open_read(stock(), date()) -> {ok, stockdb()} | {error, Reason::term()}.  
open_read(_Stock, _Date) ->
  {error, not_implemented}.

%% @doc Open stock for appending
-spec open_append(stock(), date()) -> {ok, stockdb()} | {error, Reason::term()}.  
open_append(Stock, Date) ->
  stockdb_appender:open(stockdb_fs:path(Stock, Date), [{stock,Stock},{date,Date}]).

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

-type open_options() :: append | read | raw.
-spec open(Path::file:filename(), [open_options()]) -> {ok, stockdb()} | {error, Reason::term()}.
open(Path, Options) ->
  case lists:member(raw, Options) of
    true ->
      stockdb_raw:open(Path, [raw|Options]);
    false ->
      {ok, Pid} = gen_server:start_link(stockdb_instance, {Path, Options}, []),
      {ok, {stockdb_pid, Pid}}
  end.


seek_utc({stockdb_pid, Pid}, UTC) ->
  % set buffer and state to first event after given timestamp
  gen_server:call(Pid, {seek_utc, UTC}).

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

%% @doc Return root dir of history
root() ->
  get_value(root, "db").


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       File system organization and querying
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Return path to DB file for given stock and date
path(wildcard, Date) ->
  path("*", Date);

path(Stock, Date) ->
  BaseName = [Stock, ?STOCK_DATE_DELIMITER, filename_timestamp(Date), ".stock"],
  filename:join([root(), stock, BaseName]).


%% @doc List all existing DB files
list_db() -> list_db(wildcard).

%% @doc List existing DB files for given stock
list_db(Stock) ->
  DbWildcard = path(Stock, filename_timestamp(wildcard)),
  filelib:wildcard(DbWildcard).


% %% @doc List stocks having any history data
% stocks() ->
%   StockSet = lists:foldl(fun(DbFile, Set) ->
%         {db, Stock, _Date} = file_info(DbFile),
%         sets:add_element(Stock, Set)
%     end, sets:new(), list_db()),
%   lists:sort(sets:to_list(StockSet)).


%% @doc List dates when given stock has history data
stock_dates(Stock) ->
  Dates = lists:map(fun(DbFile) ->
        {db, _Stock, Date} = file_info(DbFile),
        Date
    end, list_db(Stock)),
  lists:sort(Dates).


%% @doc List dates when all given stocks have data
common_dates(Stocks) ->
  StockDates = lists:map(fun stock_dates/1, Stocks),
  [Dates1|OtherDates] = StockDates,

  _CommonDates = lists:foldl(fun(Dates, CommonDates) ->
        Missing = CommonDates -- Dates,
        CommonDates -- Missing
    end, Dates1, OtherDates).


%% @doc return file information as {db, Stock, Date} if possible
file_info(Path) ->
  case (catch get_file_info(Path)) of
    {ok, Info} ->
      Info;
    Other ->
      ?D(Other),
      undefined
  end.

%% internal use for file_info/1
get_file_info(Path) ->
  ".stock" =  filename:extension(Path),

  Stock_Date = filename:rootname(filename:basename(Path)),
  [StockString, Date] = string:tokens(Stock_Date, ?STOCK_DATE_DELIMITER),

  {ok, {db, erlang:list_to_atom(StockString), Date}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%      Format utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc format given date for use in file name
filename_timestamp({Y, M, D}) ->
  % Format as YYYY-MM-DD
  lists:flatten(io_lib:format("~4..0B" ?DATE_DELIMITER "~2..0B" ?DATE_DELIMITER "~2..0B", [Y, M, D]));

filename_timestamp(wildcard) ->
  % Wildcard for FS querying
  "????" ?DATE_DELIMITER "??" ?DATE_DELIMITER "??";

filename_timestamp(Date) ->
  filename_timestamp(parse_date(Date)).

%% @doc Parse string with date. Argument may be in form YYYY-MM-DD or YYYY/MM/DD
parse_date(DateString) when is_list(DateString) andalso length(DateString) == 10 ->
  [Y, M, D] = lists:map(fun erlang:list_to_integer/1, string:tokens(DateString, "-/.")),
  {Y, M, D}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

run_tests() ->
  eunit:test({application, stockdb}).

-include_lib("eunit/include/eunit.hrl").
-include("stockdb_test_content.hrl").

path_test() ->
  ?assertEqual("db/stock/MICEX.TEST:2012-08-03.stock", path('MICEX.TEST', {2012, 08, 03})),
  ?assertEqual("db/stock/MICEX.TEST:2012-08-03.stock", path('MICEX.TEST', "2012-08-03")),
  ?assertEqual("db/stock/MICEX.TEST:2012-08-03.stock", path('MICEX.TEST', "2012/08/03")),
  ?assertEqual("db/stock/*:????-??-??.stock", path(wildcard, wildcard)),
  ok.

file_info_test() ->
  ?assertEqual({db, 'MICEX.TEST', "2012-08-03"}, file_info("db/stock/MICEX.TEST:2012-08-03.stock")),
  ?assertEqual(undefined, file_info("db/stock/MICEX.TEST-2012-08-03.stock")),
  ok.

seek_read_test() ->
  File = ?TEMPFILE("seek-read-test.temp"),
  write_events_to_file(File, full_content()),

  {ok, S} = ?MODULE:open(File, [read]),
  ?MODULE:seek_utc(S, 1343207300000),

  E1 = ?MODULE:read_event(S),
  ensure_packets_equal({md, 1343207307670, [{12.32, 800}, {12.170, 400}, {11.97, 1100}], [{12.440, 800}, {12.47, 1100}, {12.69, 600}]},
    E1),

  ok = file:delete(File).
