-module(stockdb_filters).
-author('Max Lapshin <max@maxidoors.ru>').

-include("../include/stockdb.hrl").
-include_lib("eunit/include/eunit.hrl").


-export([candle/2, count/2, drop/2]).
% -export([average/2]).

-record(candle, {
    type = md,
    period = 30000,
    current_segment,
    open,
    high,
    low,
    close
}).

parse_options([], #candle{} = Candle) ->
  Candle;
parse_options([{period, Period}|MoreOpts], #candle{} = Candle) ->
  parse_options(MoreOpts, Candle#candle{period = Period});
parse_options([{type, Type}|MoreOpts], #candle{} = Candle) ->
  parse_options(MoreOpts, Candle#candle{type = Type}).


candle(Event, undefined) ->
  candle(Event, []);

candle(Packet, Opts) when is_list(Opts) ->
  Candle = parse_options(Opts, #candle{}), 
  candle(Packet, Candle);

candle(#md{} = MD, #candle{type = Type} = Candle) when Type =/= md ->
  {[MD], Candle};

candle(#trade{} = Trade, #candle{type = Type} = Candle) when Type =/= trade ->
  {[Trade], Candle};

candle(eof, #candle{} = Candle) ->
  flush_segment(Candle);

candle(Unknown, #candle{} = Candle)
when not is_record(Unknown, md) andalso not is_record(Unknown, trade) ->
  {[Unknown], Candle};

candle(Packet, #candle{open = undefined, period = Period} = Candle) ->
  Segment = case Period of
    undefined -> undefined;
    Int when is_integer(Int) -> timestamp(Packet) div Period
  end,
  {[], start_segment(Segment, Packet, Candle)};

candle(Packet, #candle{period = undefined} = Candle) ->
  {[], candle_accumulate(Packet, Candle)};

candle(Packet, #candle{period = Period, current_segment = Seg} = Candle)
when is_number(Period) andalso is_record(Packet, md) orelse is_record(Packet, trade) ->
  case timestamp(Packet) div Period of
    Seg ->
      {[], candle_accumulate(Packet, Candle)};
    NewSeg ->
      {Events, Candle1} = flush_segment(Candle),
      {Events, start_segment(NewSeg, Packet, Candle1)}
  end.

start_segment(Segment, Packet, Candle) ->
  Opened = Candle#candle{current_segment = Segment, open = Packet},
  candle_accumulate(Packet, Opened).

flush_segment(#candle{open = Open, high = High, low = Low, close = Close} = Candle) ->
  Events = lists:sort([Open, High, Low, Close]),
  {Events -- [undefined], Candle#candle{
      open = undefined,
      high = undefined,
      low = undefined,
      close = undefined}
  }.


candle_accumulate(Packet, #candle{high = High, low = Low} = Candle) ->
  Candle#candle{high = highest(High, Packet), low = lowest(Low, Packet), close = Packet}.


highest(undefined, Packet) ->
  Packet;
highest(#md{ask = [{AskL, _}|_]}, #md{ask = [{AskH,_}|_]} = Highest) when AskH > AskL ->
  Highest;
highest(#trade{price = PriceL}, #trade{price = PriceH} = Highest) when PriceH > PriceL ->
  Highest;
highest(Anything, _NoMatter) ->
  Anything.


lowest(undefined, Packet) ->
  Packet;
lowest(#md{bid = [{BidH, _}|_]}, #md{bid = [{BidL,_}|_]} = Lowest) when BidH > BidL ->
  Lowest;
lowest(#trade{price = PriceH}, #trade{price = PriceL} = Lowest) when PriceH > PriceL ->
  Lowest;
lowest(Anything, _NoMatter) ->
  Anything.


timestamp(#md{timestamp = Timestamp}) -> Timestamp;
timestamp(#trade{timestamp = Timestamp}) -> Timestamp.


test_candle(Input) ->
  {MDList, _} = lists:mapfoldl(fun({Bid, Ask}, N) ->
        MD = #md{timestamp = N, bid = [{Bid,0}], ask = [{Ask,0}]},
        {MD, N+1}
    end, 1, Input),
  Events = run_filter(fun candle/2, MDList),
  [{Bid,Ask} || #md{bid = [{Bid,_}], ask = [{Ask,_}]} <- Events].

test_trade_candle(Input) ->
  {TradeList, _} = lists:mapfoldl(fun(Price, N) ->
        {#trade{timestamp = N, price = Price, volume = 1}, N+1}
    end, 1, Input),
  Events = run_filter(fun candle/2, [{type, trade}], TradeList, []),
  [Price || #trade{price = Price} <- Events].


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
  
  ?assertEqual([1, 0, 10, 8], test_trade_candle([1,2,3,0,5,10,1,8])),
  ?assertEqual([1, 0, 8], test_trade_candle([1,2,3,0,5,8])),
  ?assertEqual([1, 8], test_trade_candle([1,2,3,1,5,8])),

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

candle_pass_foreign_test() ->
  ?assertMatch({[#trade{}], #candle{}}, candle(#trade{}, [{type, md}])),
  ?assertMatch({[#md{}], #candle{}}, candle(#md{}, [{type, trade}])),
  ok.


count(eof, Count) ->
  {[Count], Count};
count(_Event, Count) ->
  {[], Count + 1}.

drop(#md{}, md) ->
  {[], md};
drop(#trade{}, trade) ->
  {[], trade};
drop(eof, What) ->
  {[], What};
drop(Other, What) ->
  {[Other], What}.
