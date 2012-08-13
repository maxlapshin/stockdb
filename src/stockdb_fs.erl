%%% @doc stockdb_fs
%%% This module handles DB file structure

-module(stockdb_fs).
-author({"Danil Zagoskin", z@gosk.in}).

-include("../include/stockdb.hrl").

% Configuration
-export([root/0]).

% Filename <-> {Stock, Date} conversion
-export([path/2, file_info/1, parse_date/1]).

% Local querying
-export([stocks/0, dates/1, common_dates/1]).

% Remote querying
-export([stocks/1, dates/2, common_dates/2]).



%% @doc Return root dir of history
root() ->
  stockdb:get_value(root, "db").


%% @doc Return path to DB file for given stock and date
path(wildcard, Date) ->
  path('*', Date);

path(Stock, Date) when is_atom(Stock) ->
  path({stock, Stock}, Date);

path({path, Path}, _Date) ->
  Path;

path({Type, Stock}, Date) ->
  BaseName = [Stock, "-", filename_timestamp(Date), ".stock"],
  filename:join([root(), Type, BaseName]).



%% @doc List of stocks in local database
-spec stocks() -> [stock()].
stocks() ->
  StockSet = lists:foldl(fun(DbFile, Set) ->
        {db, Stock, _Date} = file_info(DbFile),
        sets:add_element(Stock, Set)
    end, sets:new(), list_db()),
  lists:sort(sets:to_list(StockSet)).

%% @doc List of stocks in remote database
-spec stocks(Storage::term()) -> [stock()].
stocks(_Storage) ->
  [].


%% @doc List of available dates for stock
-spec dates(stock()) -> [date()].
dates(Stock) ->
  Dates = lists:map(fun(DbFile) ->
        {db, _Stock, Date} = file_info(DbFile),
        Date
    end, list_db(Stock)),
  lists:sort(Dates).

%% @doc List of available dates in remote database
-spec dates(Storage::term(), Stock::stock()) -> [date()].
dates(_Storage, _Stock) ->
  [].


%% @doc List dates when all given stocks have data
-spec common_dates([stock()]) -> [date()].
common_dates(Stocks) ->
  StockDates = lists:map(fun dates/1, Stocks),
  [Dates1|OtherDates] = StockDates,

  _CommonDates = lists:foldl(fun(Dates, CommonDates) ->
        Missing = CommonDates -- Dates,
        CommonDates -- Missing
    end, Dates1, OtherDates).

common_dates(_Storage, _Stocks) ->
  [].

%% @doc List all existing DB files
list_db() -> list_db(wildcard).

%% @doc List existing DB files for given stock
list_db(Stock) ->
  DbWildcard = path(Stock, wildcard),
  filelib:wildcard(DbWildcard).

%% @doc return file information as {db, Stock, Date} if possible
file_info(Path) ->
  case (catch get_file_info(Path)) of
    {ok, Info} ->
      Info;
    _Other ->
      % ?D(Other),
      undefined
  end.

%% internal use for file_info/1
get_file_info(Path) ->
  {match, [StockString, Date]} = re:run(filename:basename(Path), "^(.*)-(\\d{4}-\\d{2}-\\d{2})\.stock$", [{capture, [1, 2], list}]),
  {ok, {db, erlang:list_to_atom(StockString), Date}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%      Format utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc format given date for use in file name
filename_timestamp({Y, M, D}) ->
  % Format as YYYY-MM-DD
  lists:flatten(io_lib:format("~4..0B-~2..0B-~2..0B", [Y, M, D]));

filename_timestamp(wildcard) ->
  % Wildcard for FS querying
  "????-??-??";

filename_timestamp(Date) ->
  filename_timestamp(parse_date(Date)).

%% @doc Parse string with date. Argument may be in form YYYY-MM-DD or YYYY/MM/DD
parse_date({Y,M,D}) ->
  {Y,M,D};

parse_date(DateString) when is_list(DateString) andalso length(DateString) == 10 ->
  [Y, M, D] = lists:map(fun erlang:list_to_integer/1, string:tokens(DateString, "-/.")),
  {Y, M, D}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%     TESTING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("eunit/include/eunit.hrl").

path_test() ->
  application:set_env(stockdb, root, "custom/path"),
  ?assertEqual("custom/path/stock/MICEX.TEST-2012-08-03.stock", path('MICEX.TEST', {2012, 08, 03})),
  ?assertEqual("custom/path/stock/MICEX.TEST-2012-08-03.stock", path('MICEX.TEST', "2012-08-03")),
  ?assertEqual("custom/path/stock/MICEX.TEST-2012-08-03.stock", path('MICEX.TEST', "2012/08/03")),
  ?assertEqual("custom/path/stock/*-????-??-??.stock", path(wildcard, wildcard)),
  ok.

file_info_test() ->
  application:set_env(stockdb, root, "custom/path"),
  ?assertEqual({db, 'MICEX.TEST', "2012-08-03"}, file_info("db/stock/MICEX.TEST-2012-08-03.stock")),
  ?assertEqual(undefined, file_info("db/stock/MICEX.TEST.2012-08-03.stock")),
  ?assertEqual(undefined, file_info("db/stock/MICEX.TEST-2012.08.03.stock")),
  ok.

stocks_test() ->
  application:set_env(stockdb, root, code:lib_dir(stockdb, test) ++ "/fixtures/fs"),
  ?assertEqual(lists:sort(['MICEX.TEST', 'LSEIOB.TEST', 'FX_TOM.USDRUB']), stocks()),
  ok.

dates_test() ->
  application:set_env(stockdb, root, code:lib_dir(stockdb, test) ++ "/fixtures/fs"),
  ?assertEqual(["2012-08-01", "2012-08-02", "2012-08-05"],
    dates('MICEX.TEST')),
  ?assertEqual(["2012-08-01", "2012-08-03", "2012-08-04", "2012-08-05", "2012-08-06"],
    dates('LSEIOB.TEST')),
  ?assertEqual(["2012-08-02", "2012-08-04", "2012-08-05"],
    dates('FX_TOM.USDRUB')),
  ok.

common_dates_test() ->
  application:set_env(stockdb, root, code:lib_dir(stockdb, test) ++ "/fixtures/fs"),
  ?assertEqual(["2012-08-05"], common_dates(['MICEX.TEST', 'LSEIOB.TEST', 'FX_TOM.USDRUB'])),
  ok.

