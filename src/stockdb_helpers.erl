-module(stockdb_helpers).
-include("../include/stockdb.hrl").
-include("stockdb.hrl").
-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([candle/3]).

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
