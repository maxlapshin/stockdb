%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(stockdb).
-author({"Danil Zagoskin", z@gosk.in}).
-include("../include/stockdb.hrl").
-include("log.hrl").

%% Application configuration
-export([get_value/1, get_value/2, root/0]).

%% File-like API
-export([open/2, append/2, seek_utc/2, read_event/1, close/1]).

%% Filesystem querying
-export([path/2, list_db/0, list_db/1, stocks/0, stock_dates/1, common_dates/1, file_info/1]).

%% DB range processing
-export([foldl/4]).

%% Run tests
-export([run_tests/0]).

-define(DATE_DELIMITER, "-").
-define(STOCK_DATE_DELIMITER, ":").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%        File API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-spec open(Path::nonempty_string()) -> {ok, pid()} | {error, Reason::term()}.
open(Path, Modes) ->
  gen_server:start_link(stockdb_instance, {Path, Modes}, []).

%% Proxy requests to instance
close(Pid) ->
  % terminate instance
  gen_server:call(Pid, stop).

append(Pid, Object) ->
  % append object to DB
  gen_server:call(Pid, {append, Object}).

read_event(Pid) ->
  % pop event from buffer
  gen_server:call(Pid, read_event).

seek_utc(Pid, UTC) ->
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


%% @doc List stocks having any history data
stocks() ->
  StockSet = lists:foldl(fun(DbFile, Set) ->
        {db, Stock, _Date} = file_info(DbFile),
        sets:add_element(Stock, Set)
    end, sets:new(), list_db()),
  lists:sort(sets:to_list(StockSet)).


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
