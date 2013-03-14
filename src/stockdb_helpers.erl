-module(stockdb_helpers).
-include("../include/stockdb.hrl").
-include("stockdb.hrl").
-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([candle/3]).
-export([timestamp/1]).
-export([md_at/2]).

-spec candle(stockdb:stock(), stockdb:date(), list(reader_option())) -> {stockdb:price(),stockdb:price(),stockdb:price(),stockdb:price()}.
candle(Stock, Date, Options) ->
  [{candle,Candle}] = stockdb:info(Stock, Date, [candle]),
  if Options == [] andalso Candle =/= undefined ->
    Candle;
    true -> try calculate_candle(Stock, Date, Options) of
      Result -> Result
    catch
      Class:Error -> erlang:raise(Class, {Error, candle, Stock, Date}, erlang:get_stacktrace())
    end
  end.

calculate_candle(Stock, Date, Options) ->
  {ok, Iterator} = stockdb:init_reader(Stock, Date, [{filter, candle, [{period, 24*3600*1000}]}|Options]),
  % Events1 = stockdb:events(Stock, Date),
  % Events = [mid(E) || E <- Events1, element(1,E) == trade],
  Events = [mid(E) || E <- stockdb:events(Iterator)],
  Open = lists:nth(1, Events),
  Close = lists:nth(length(Events), Events),
  High = lists:max(Events),
  Low = lists:min(Events),
  {Open, High, Low, Close}.


mid(#trade{price = Price}) -> Price;
mid(#md{bid = [{Bid,_}|_], ask = [{Ask,_}|_]}) -> (Bid + Ask) / 2.


% Convert seconds to milliseconds
timestamp(UnixTime) when is_integer(UnixTime), UnixTime < 4000000000 ->
  UnixTime * 1000;

% No convertion needed
timestamp(UTC) when is_integer(UTC) ->
  UTC;

% Convert given {Date, Time} or {Megasec, Sec, Microsec} to millisecond timestamp
timestamp({{_Y,_Mon,_D} = Day,{H,Min,S}}) ->
  timestamp({Day, {H,Min,S, 0}});

timestamp({{_Y,_Mon,_D} = Day,{H,Min,S, Milli}}) ->
  GregSeconds_Zero = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds_Now = calendar:datetime_to_gregorian_seconds({Day,{H,Min,S}}),
  (GregSeconds_Now - GregSeconds_Zero)*1000 + Milli;

timestamp({Megaseconds, Seconds, Microseconds}) ->
  (Megaseconds*1000000 + Seconds)*1000 + Microseconds div 1000.


date_time(Timestamp) ->
  GregSeconds_Zero = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds = GregSeconds_Zero + Timestamp div 1000,
  calendar:gregorian_seconds_to_datetime(GregSeconds).


% get MD of given stock at given time
md_at(Stock, Time) ->
  Timestamp = timestamp(Time),
  {Date, _} = date_time(Timestamp),
  [MD] = stockdb:events(Stock, Date, [{range, Timestamp - timer:minutes(15), Timestamp},
      {filter, drop, trade}, {filter, last, md}]),
  MD.
