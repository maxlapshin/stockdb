-module(stockdb_helpers).
-include("../include/stockdb.hrl").
-include("stockdb.hrl").
-include("log.hrl").

-export([candle/3]).

-spec candle(stock(), date(), list(reader_option())) -> {price(),price(),price(),price()}.
candle(Stock, Date, Options) ->
  {ok, Iterator} = stockdb:init_reader(Stock, Date, [{filter, candle, [{period, 24*3600*1000}]}|Options]),
  Events = stockdb:events(Iterator),
  Open = mid(lists:nth(1, Events)),
  Close = mid(lists:nth(length(Events), Events)),
  High = lists:max([mid(MD) || #md{} = MD <- Events]),
  Low = lists:min([mid(MD) || #md{} = MD <- Events]),
  {Open, High, Low, Close}.


mid(#md{bid = [{Bid,_}|_], ask = [{Ask,_}|_]}) -> (Bid + Ask) / 2.
