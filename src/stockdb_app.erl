-module(stockdb_app).
-export([start/2, stop/1]).

start(_, _) ->
  stockdb_sup:start_link().

stop(_) ->
  stockdb_sup:stop().
