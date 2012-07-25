%%% @doc Binary row format for stockdb
-module(stockdb_format).
-author({"Danil Zagoskin", z@gosk.in}).

-export([encode_full_md/2, encode_delta_md/2]).
-export([encode_trade/3, decode_trade/1]).
-export([decode_timestamp/1]).
-export([packet_type/1, decode_full_md/2, decode_delta_md/2]).
-export([format_header_value/2, parse_header_value/2]).

nested_foldl(Fun, Acc0, List) when is_list(List) ->
  lists:foldl(fun(E, Acc) -> nested_foldl(Fun, Acc, E) end, Acc0, List);
nested_foldl(Fun, Acc, Element) ->
  Fun(Element, Acc).

-spec encode_full_md(Timestamp::integer(), BidAsk::[{Price::integer(), Volume::integer()}]) -> iolist().
encode_full_md(Timestamp, BidAsk) ->
  nested_foldl(fun({Price, Volume}, Acc) ->
        <<Acc/binary, Price:32/integer, Volume:32/integer>>
    end, <<1:1, 0:1, Timestamp:62/integer>>, BidAsk).

encode_delta_md(TimeDelta, BidAskDelta) ->
  ETimeDelta = leb128:encode(TimeDelta),

  Unpadded = nested_foldl(fun({DPrice, DVolume}, Acc) ->
        PriceBits = encode_delta_value(DPrice),
        VolumeBits = encode_delta_value(DVolume),
        <<Acc/bitstring, PriceBits/bitstring, VolumeBits/bitstring>>
    end, <<0:1, ETimeDelta/binary>>, BidAskDelta),
  
  TailBits = erlang:bit_size(Unpadded) rem 8,
  MissingBits = (8 - TailBits) rem 8,

  <<Unpadded/bitstring, 0:MissingBits/integer>>.

encode_trade(Timestamp, Price, Volume) ->
  <<1:1, 1:1, Timestamp:62/integer, Price:32/integer, Volume:32/integer>>.

encode_delta_value(0) -> <<0:1>>;
encode_delta_value(V) -> <<1:1, (leb128:encode_signed(V))/bitstring>>.


packet_type(<<1:1, 0:1, _Tail/bitstring>>) -> full_md;
packet_type(<<1:1, 1:1, _Tail/bitstring>>) -> trade;
packet_type(<<0:1, _Tail/bitstring>>) -> delta_md.

decode_timestamp(<<1:1, _:1/integer, Timestamp:62/integer, _Tail/bitstring>>) ->
  Timestamp.


decode_full_md(<<1:1, Timestamp:63/integer, BidAskTail/bitstring>>, Depth) ->
  {Bid, AskTail} = decode_full_bidask(BidAskTail, Depth),
  {Ask, Tail} = decode_full_bidask(AskTail, Depth),
  {Timestamp, [Bid, Ask], Tail}.

decode_full_bidask(Tail, 0) ->
  {[], Tail};
decode_full_bidask(<<Price:32/integer, Volume:32/integer, Tail/bitstring>>, Depth) ->
  Line = {Price, Volume},
  {TailLines, FinalTail} = decode_full_bidask(Tail, Depth - 1),
  {[Line | TailLines], FinalTail}.


decode_delta_md(<<0:1, TsBidAskTail/bitstring>>, Depth) ->
  {Timestamp, BidAskTail} = leb128:decode(TsBidAskTail),
  {Bid, AskTail} = decode_delta_bidask(BidAskTail, Depth),
  {Ask, Tail} = decode_delta_bidask(AskTail, Depth),
  ExtraBits = erlang:bit_size(Tail) rem 8,
  <<0:ExtraBits/integer, AlignedTail/binary>> = Tail,

  {Timestamp, [Bid, Ask], AlignedTail}.

decode_delta_bidask(Tail, 0) ->
  {[], Tail};
decode_delta_bidask(PVTail, Depth) ->
  {DPrice, VTail} = decode_delta_field(PVTail),
  {DVolume, Tail} = decode_delta_field(VTail),
  DLine = {DPrice, DVolume},
  {TailDLines, FinalTail} = decode_delta_bidask(Tail, Depth - 1),
  {[DLine | TailDLines], FinalTail}.

decode_delta_field(<<0:1, Tail/bitstring>>) ->
  {0, Tail};
decode_delta_field(<<1:1, ValueTail/bitstring>>) ->
  leb128:decode_signed(ValueTail).


decode_trade(<<1:1, 1:1, Timestamp:62/integer, Price:32/integer, Volume:32/integer, Tail/bitstring>>) ->
  {Timestamp, Price, Volume, Tail}.


format_header_value(date, {Y, M, D}) ->
  io_lib:format("~4..0B/~2..0B/~2..0B", [Y, M, D]);

format_header_value(stock, Stock) ->
  erlang:atom_to_list(Stock);

format_header_value(_, Value) ->
  io_lib:print(Value).


parse_header_value(depth, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(scale, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(chunk_size, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(version, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(date, DateStr) ->
  [YS, MS, DS] = string:tokens(DateStr, "/"),
  { erlang:list_to_integer(YS),
    erlang:list_to_integer(MS),
    erlang:list_to_integer(DS)};

parse_header_value(stock, StockStr) ->
  erlang:list_to_atom(StockStr);

parse_header_value(_, Value) ->
  Value.

% EUnit tests
-include_lib("eunit/include/eunit.hrl").

full_md_test() ->
  Timestamp = 16#138BDF77CBA,
  BidAsk = [[{1530, 250}, {1520, 111}], [{1673, 15}, {1700, 90}]],
  Bin = <<16#80000138BDF77CBA:64/integer,
    1530:32/integer, 250:32/integer,  1520:32/integer, 111:32/integer,
    1673:32/integer, 15:32/integer,   1700:32/integer, 90:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(full_md, packet_type(Bin)),
  ?assertEqual(Bin, encode_full_md(Timestamp, BidAsk)),
  ?assertEqual({Timestamp, BidAsk, Tail}, decode_full_md(<<Bin/binary, Tail/bitstring>>, 2)).

delta_md_test() ->
  DTimestamp = 270,
  DBidAsk = [[{0, -4}, {0, 0}], [{-230, 100}, {-100, -47}]],
  Bin = <<0:1, 142,2,  0:1, 1:1,124,  0:1, 0:1,  1:1,154,126, 1:1,228,0,  1:1,156,127,  1:1,81,  0:7>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(delta_md, packet_type(Bin)),
  ?assertEqual(Bin, encode_delta_md(DTimestamp, DBidAsk)),
  ?assertEqual({DTimestamp, DBidAsk, Tail}, decode_delta_md(<<Bin/bitstring, Tail/bitstring>>, 2)).

trade_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = 16#DEAD,
  Volume = 16#BEEF,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/integer, Volume:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(trade, packet_type(Bin)),
  ?assertEqual(Bin, encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, Tail}, decode_trade(<<Bin/binary, Tail/binary>>)).

timestamp_test() ->
  ?assertEqual(16#138BDF77CBA, decode_timestamp(<<16#80000138BDF77CBA:64/integer, 7, 239, 183, 19>>)),
  ?assertEqual(16#138BDF77CBA, decode_timestamp(<<16#C0000138BDF77CBA:64/integer, 7, 239, 183, 19>>)).

format_header_value_test() ->
  ?assertEqual("2012/07/19", lists:flatten(format_header_value(date, {2012, 7, 19}))),
  ?assertEqual("MICEX.URKA", lists:flatten(format_header_value(stock, 'MICEX.URKA'))),
  ?assertEqual("17", lists:flatten(format_header_value(depth, 17))),
  ?assertEqual("17", lists:flatten(format_header_value(scale, 17))),
  ?assertEqual("17", lists:flatten(format_header_value(chunk_size, 17))),
  ?assertEqual("17", lists:flatten(format_header_value(version, 17))).

parse_header_value_test() ->
  ?assertEqual({2012, 7, 19}, parse_header_value(date, "2012/07/19")),
  ?assertEqual('MICEX.URKA', parse_header_value(stock, "MICEX.URKA")),
  ?assertEqual(17, parse_header_value(depth, "17")),
  ?assertEqual(17, parse_header_value(scale, "17")),
  ?assertEqual(17, parse_header_value(chunk_size, "17")),
  ?assertEqual(17, parse_header_value(version, "17")).
