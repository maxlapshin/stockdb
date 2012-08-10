-module(stockdb_filters).
-author('Max Lapshin <max@maxidoors.ru>').

-include("../include/stockdb.hrl").
-include_lib("eunit/include/eunit.hrl").


-export([candle/2]).
% -export([average/2]).

-record(candle, {
  granulation,
  current_segment,
  open,
  high_ask,
  low_bid,
  close
}).

-define(GRANULATION, 30000).

candle(Event, Candle) when not is_record(Event, md) andalso Event =/= eof ->
  {[Event], Candle};

candle(Event, undefined) ->
  candle(Event, []);

candle(#md{timestamp = Timestamp} = MD, Opts) when is_list(Opts) ->
  Granulation = proplists:get_value(granulation, Opts, ?GRANULATION),
  candle(MD, #candle{
    granulation = Granulation,
    current_segment = Timestamp div Granulation,
    open = MD,
    high_ask = MD,
    low_bid = MD,
    close = MD
    });

candle(#md{timestamp = Timestamp} = MD, #candle{granulation = Granulation, current_segment = Seg} = Candle) 
  when Timestamp div Granulation > Seg ->
  {Events, Candle1} = flush_segment(Candle),
  {Events, Candle1#candle{current_segment = Timestamp div Granulation, open = MD}};

candle(eof, #candle{} = Candle) ->
  {Events, NewCandle} = flush_segment(Candle),
  {Events ++ [eof], NewCandle};

candle(#md{} = MD, #candle{} = Candle) ->
  {[], accumulate_md(MD, Candle)}.


flush_segment(#candle{open = Open, high_ask = HighAsk, low_bid = LowBid, close = Close} = Candle) ->
  Events = lists:usort([Open, HighAsk, LowBid, Close]),
  {Events, Candle#candle{open = undefined, high_ask = undefined, low_bid = undefined, close = undefined}}.


accumulate_md(#md{bid = [{Bid,_}|_], ask = [{Ask,_}|_]} = MD, #candle{high_ask = HighAsk, low_bid = LowBid} = Candle) ->
  HA1 = case HighAsk of
    undefined -> MD;
    #md{ask = [{Ask1,_}|_]} when Ask > Ask1 -> MD;
    _ -> HighAsk
  end,

  LB1 = case LowBid of
    undefined -> MD;
    #md{bid = [{Bid1,_}|_]} when Bid < Bid1 -> MD;
    _ -> LowBid
  end,
  
  Candle#candle{high_ask = HA1, low_bid = LB1, close = MD}.




test_candle(Input) ->
  MDList = lists:map(fun(N) ->
    {Bid,Ask} = lists:nth(N,Input),
    {md, N, [{Bid,0}], [{Ask,0}]}
  end, lists:seq(1,length(Input))),
  Events = run_filter(fun candle/2, MDList),
  [{Bid,Ask} || {md, _, [{Bid,_}], [{Ask,_}]} <- Events].


run_filter(Fun, List) ->
  run_filter(Fun, undefined, List, []).

run_filter(Fun, State, [Event|List], Acc) ->
  {Events, State1} = Fun(Event, State),
  run_filter(Fun, State1, List, Acc ++ Events);

run_filter(Fun, State, [], Acc) ->
  {Events, _State1} = Fun(eof, State),
  Acc ++ Events.

candle_test() ->
  ?assertEqual([{1,5}, {0,4}, {10,14}, {8,12}], test_candle([{1,5},{2,8},{3,4},{0,4},{5,11},{10,14},{1,9},{8,12}])),
  ?assertEqual([{1,5}, {0,4}, {8,12}], test_candle([{1,5},{2,8},{3,4},{0,4},{5,11},{8,12}])),
  ?assertEqual([{1,5}, {8,12}], test_candle([{1,5},{2,8},{3,4},{5,11},{8,12}])),
  
  N = 100000,
  List = [begin
    Bid = random:uniform(1000),
    Ask = Bid + random:uniform(20),
    {md,I, [{Bid,0}], [{Ask,0}]}
  end || I <- lists:seq(1,N)],
  T1 = erlang:now(),
  S2 = lists:foldl(fun(MD, S) ->
    {_, S1} = candle(MD, S),
    S1
  end, undefined, List),
  {_, _} = candle(eof, S2),
  T2 = erlang:now(),
  Delta = timer:now_diff(T2,T1),
  ?debugFmt("Candle bm: ~B: ~B ms", [N, Delta]),
  ok.



