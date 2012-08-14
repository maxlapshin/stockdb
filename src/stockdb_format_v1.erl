%%% @doc Binary row format for stockdb
-module(stockdb_format_v1).
-author({"Danil Zagoskin", z@gosk.in}).

-include("stockdb.hrl").


-include_lib("eunit/include/eunit.hrl").
-export([encode_full_md/2, encode_delta_md/2]).
-export([encode_trade/3, decode_trade/1]).
-export([packet_type/1, decode_full_md/2, decode_delta_md/2]).
-export([format_header_value/2, parse_header_value/2]).

%% Read all packets from buffer
-export([read_all_packets/2]).




nested_foldl(Fun, Acc0, List) when is_list(List) ->
  lists:foldl(fun(E, Acc) -> nested_foldl(Fun, Acc, E) end, Acc0, List);
nested_foldl(Fun, Acc, Element) ->
  Fun(Element, Acc).

-spec encode_full_md(Timestamp::integer(), BidAsk::[{Price::integer(), Volume::integer()}]) -> iolist().
encode_full_md(Timestamp, BidAsk) ->
  nested_foldl(fun append_full_PV/2, <<1:1, 0:1, Timestamp:62/integer>>, BidAsk).

append_full_PV({Price, Volume}, Acc) when is_integer(Price) andalso is_integer(Volume) andalso Volume >= 0 ->
  <<Acc/binary, Price:32/signed-integer, Volume:32/unsigned-integer>>.

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

encode_trade(Timestamp, Price, Volume) when is_integer(Price) andalso is_integer(Volume) andalso Volume >= 0 ->
  <<1:1, 1:1, Timestamp:62/integer, Price:32/signed-integer, Volume:32/unsigned-integer>>.

encode_delta_value(0) -> <<0:1>>;
encode_delta_value(V) -> <<1:1, (leb128:encode_signed(V))/bitstring>>.



packet_type(<<1:1, 0:1, _Tail/bitstring>>) -> full_md;
packet_type(<<1:1, 1:1, _Tail/bitstring>>) -> trade;
packet_type(<<0:1, _Tail/bitstring>>) -> delta_md.



decode_full_md(<<1:1, Timestamp:63/integer, BidAskTail/binary>>, Depth) ->
  {Bid, AskTail} = decode_full_bidask(BidAskTail, Depth),
  {Ask, Tail} = decode_full_bidask(AskTail, Depth),
  {Timestamp, [Bid, Ask], Tail}.

decode_full_bidask(Tail, 0) ->
  {[], Tail};
decode_full_bidask(<<Price:32/signed-integer, Volume:32/unsigned-integer, Tail/binary>>, Depth) ->
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


decode_trade(<<1:1, 1:1, Timestamp:62/integer, Price:32/signed-integer, Volume:32/unsigned-integer, Tail/binary>>) ->
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

read_all_packets(Buffer, #dbstate{} = State0) when is_binary(Buffer) ->
  read_all_packets(Buffer, State0, []).

read_all_packets(<<>>, _State, RevPackets) ->
  lists:reverse(RevPackets);

read_all_packets(Buffer, State, RevPackets) ->
  try
    {Packet, Tail, NewState} = get_first_packet(Buffer, State),
    read_all_packets(Tail, NewState, [Packet|RevPackets])
  catch
    _:_ ->
      lists:reverse(RevPackets)
  end.

get_first_packet(Buffer, #dbstate{depth = Depth, last_bidask = LastBidAsk, last_timestamp = LastTimestamp, scale = Scale} = State) ->
  case packet_type(Buffer) of
    full_md ->
      {Timestamp, BidAsk, Tail} = decode_full_md(Buffer, Depth),

      {packet_from_mdentry(Timestamp, BidAsk, State), Tail,
        State#dbstate{last_timestamp = Timestamp, last_bidask = BidAsk}};
    delta_md ->
      {DTimestamp, DBidAsk, Tail} = decode_delta_md(Buffer, Depth),
      BidAsk = bidask_delta_apply(LastBidAsk, DBidAsk),
      Timestamp = LastTimestamp + DTimestamp,

      {packet_from_mdentry(Timestamp, BidAsk, State), Tail,
        State#dbstate{last_timestamp = Timestamp, last_bidask = BidAsk}};
    trade ->
      {Timestamp, Price, Volume, Tail} = decode_trade(Buffer),
      {{trade, Timestamp, Price/Scale, Volume}, Tail,
        State#dbstate{last_timestamp = Timestamp}}
  end.

bidask_delta_apply([[_|_] = Bid1, [_|_] = Ask1], [[_|_] = Bid2, [_|_] = Ask2]) ->
  [bidask_delta_apply1(Bid1, Bid2), bidask_delta_apply1(Ask1, Ask2)].

bidask_delta_apply1(List1, List2) ->
  lists:zipwith(fun({Price, Volume}, {DPrice, DVolume}) ->
    {Price + DPrice, Volume + DVolume}
  end, List1, List2).

split_bidask([Bid, Ask], _Depth) ->
  {Bid, Ask}.


packet_from_mdentry(Timestamp, BidAsk, #dbstate{depth = Depth, scale = Scale}) ->
  {Bid, Ask} = split_bidask(BidAsk, Depth),
  SBid = if is_number(Scale) -> apply_scale(Bid, 1/Scale); true -> Bid end,
  SAsk = if is_number(Scale) -> apply_scale(Ask, 1/Scale); true -> Ask end,
  {md, Timestamp, SBid, SAsk}.

apply_scale(PVList, Scale) when is_float(Scale) ->
  lists:map(fun({Price, Volume}) ->
        {Price * Scale, Volume}
    end, PVList).
