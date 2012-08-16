-module(stockdb_helpers).
-include("../include/stockdb.hrl").
-include("stockdb.hrl").
-include("log.hrl").

-export([candle/3]).

-spec candle(stock(), date(), list(reader_option())) -> {price(),price(),price(),price()}.
candle(Stock, Date, Options) ->
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
