%%% @doc stockdb_format: module that codes and decodes 
%%% actual data to/from binary representation.
%%% Format version: 2
%%% Here "changed" flags for delta fields are aggregated at
%%% packet start to improve code/decode performance by
%%% byte-aligning LEB128 parts

-module(stockdb_format).
-author({"Danil Zagoskin", z@gosk.in}).

-include_lib("eunit/include/eunit.hrl").

%-on_load(init_nif/0).
%-export([read_one_row/2, read_one_row/3, read_one_row/4]).

-export([encode_full_md/2, decode_full_md/2]).
-export([encode_delta_md/2, decode_delta_md/2]).
-export([encode_trade/3, decode_trade/1]).
-export([format_header_value/2, parse_header_value/2]).

-export([decode_packet/2]).


init_nif() ->
  Path = filename:dirname(code:which(?MODULE)) ++ "/../priv",
  Load = erlang:load_nif(Path ++ "/stockdb_format", 0),
  case Load of
    ok -> ok;
    {error, {Reason,Text}} -> io:format("Load stockdb_format failed. ~p:~p~n", [Reason, Text])
  end,
  ok.

%% Utility: lists module does not have this
nested_foldl(Fun, Acc0, List) when is_list(List) ->
  lists:foldl(fun(E, Acc) -> nested_foldl(Fun, Acc, E) end, Acc0, List);
nested_foldl(Fun, Acc, Element) ->
  Fun(Element, Acc).



-spec encode_full_md(Timestamp::integer(), BidAsk::[{Price::integer(), Volume::integer()}]) -> iolist().
encode_full_md(Timestamp, BidAsk) ->
  nested_foldl(fun append_full_PV/2, <<1:1, 0:1, Timestamp:62/integer>>, BidAsk).

append_full_PV({Price, Volume}, Acc) when is_integer(Price) andalso is_integer(Volume) andalso Volume >= 0 ->
  <<Acc/binary, Price:32/signed-integer, Volume:32/unsigned-integer>>.

-spec decode_full_md(Buffer::binary(), Depth::integer()) ->
  {Timestamp::integer(), BidAsk::[{Price::integer(), Volume::integer()}], ByteCount::integer()}.
decode_full_md(<<1:1, Timestamp:63/integer, BidAskTail/binary>>, Depth) ->
  {Bid, AskTail} = decode_full_bidask(BidAskTail, Depth),
  {Ask, _Tail} = decode_full_bidask(AskTail, Depth),
  {Timestamp, [Bid, Ask], 8 + 2*2*Depth*4}.

decode_full_bidask(Tail, 0) ->
  {[], Tail};
decode_full_bidask(<<Price:32/signed-integer, Volume:32/unsigned-integer, Tail/binary>>, Depth) ->
  Line = {Price, Volume},
  {TailLines, FinalTail} = decode_full_bidask(Tail, Depth - 1),
  {[Line | TailLines], FinalTail}.


-spec encode_delta_md(TimeDelta::integer(), BidAskDelta::[{DPrice::integer(), DVolume::integer()}]) -> iolist().
encode_delta_md(TimeDelta, BidAskDelta) ->
  % Bit mask length is 4*Depth, so wee can align it to 4 bits, leaving extra space for future
  Header = <<0:4/integer>>,
  TimeBin = leb128:encode(TimeDelta),
  {HBitMask, DataBin} = nested_foldl(fun({DPrice, DVolume}, {_Bitmask, _DataBin} = AccIn) ->
        PriceAdded = add_delta_field(DPrice, AccIn),
        add_delta_field(DVolume, PriceAdded)
    end, {Header, <<>>}, BidAskDelta),
  HBitMaskPadded = pad_to_octets(HBitMask),
  <<HBitMaskPadded/binary, TimeBin/binary, DataBin/binary>>.

%% Utility: append bit to bitmask and (if not zero) value to data accumulator
add_delta_field(0, {BitMask, DataBin}) ->
  % Zero value. Append 0 to bitmask
  {<<BitMask/bitstring, 0:1>>, DataBin};

add_delta_field(Value, {BitMask, DataBin}) ->
  % non-zero value. Append 1 to bitmask and binary value to data
  ValueBin = leb128:encode_signed(Value),
  {<<BitMask/bitstring, 1:1>>, <<DataBin/binary, ValueBin/binary>>}.

pad_to_octets(BS) ->
  PadSize = erlang:byte_size(BS)*8 - erlang:bit_size(BS),
  <<BS/bitstring, 0:PadSize>>.


-spec decode_delta_md(Buffer::binary(), Depth::integer()) ->
  {TimeDelta::integer(), BidAskDelta::[{DPrice::integer(), DVolume::integer()}], ByteCount::integer()}.
decode_delta_md(<<0:1, _/bitstring>> = Bin, Depth) ->
  % Calculate bitmask size
  HalfBMSize = Depth * 2,
  % Actually, Size - 4, but it will fail with zero depth
  BMPadSize = (2*HalfBMSize + 4) rem 8,
  % Parse packet
  <<_:4, BidBitMask:HalfBMSize/bitstring, AskBitMask:HalfBMSize/bitstring, _:BMPadSize/bitstring, DataTail/binary>> = Bin,
  {TimeDelta, DBA_Tail} = leb128:decode(DataTail),
  {DBid, DA_Tail} = decode_PVs(DBA_Tail, BidBitMask),
  {DAsk, Tail} = decode_PVs(DA_Tail, AskBitMask),
  ByteCount = erlang:byte_size(Bin) - erlang:byte_size(Tail),
  {TimeDelta, [DBid, DAsk], ByteCount}.

decode_PVs(DataBin, BitMask) ->
  {RevPVs, Tail} = lists:foldl(fun({PF, VF}, {PVs, PVData}) ->
        {P, VData} = get_delta_field(PF, PVData),
        {V, Data} = get_delta_field(VF, VData),
        {[{P, V} | PVs], Data}
    end, {[], DataBin}, [{PF, VF} || <<PF:1, VF:1>> <= BitMask]),
  {lists:reverse(RevPVs), Tail}.

get_delta_field(0, Data) ->
  {0, Data};
get_delta_field(1, Data) ->
  leb128:decode_signed(Data).


-spec encode_trade(Timestamp::integer(), Price::integer(), Volume::integer()) -> iolist().
encode_trade(Timestamp, Price, Volume) when is_integer(Price) andalso is_integer(Volume) andalso Volume >= 0 ->
  <<1:1, 1:1, Timestamp:62/integer, Price:32/signed-integer, Volume:32/unsigned-integer>>.

-spec decode_trade(Buffer::binary()) -> {Timestamp::integer(), Price::integer(), Volume::integer(), ByteCount::integer()}.
decode_trade(<<1:1, 1:1, Timestamp:62/integer, Price:32/signed-integer, Volume:32/unsigned-integer, _/binary>>) ->
  {Timestamp, Price, Volume, 16}.


%% @doc Main decoding function: takes binary and depth, returns packet type, body and size
-spec decode_packet(Bin::binary(), Depth::integer()) -> {Type::full_md|delta_md|trade, PacketBody::term(), Size::integer()}.
decode_packet(<<1:1, 0:1, _/bitstring>> = Bin, Depth) ->
  {Timestamp, BidAsk, Size} = decode_full_md(Bin, Depth),
  {full_md, {Timestamp, BidAsk}, Size};
decode_packet(<<0:1, _/bitstring>> = Bin, Depth) ->
  {TimeDelta, DBidAsk, Size} = decode_delta_md(Bin, Depth),
  {delta_md, {TimeDelta, DBidAsk}, Size};
decode_packet(<<1:1, 1:1, _/bitstring>> = Bin, _Depth) ->
  {Timestamp, Price, Volume, Size} = decode_trade(Bin),
  {trade, {Timestamp, Price, Volume}, Size}.


read_one_row(_Bin, _Depth) ->
  erlang:error(nif_not_loaded).

read_one_row(_Bin, _Depth, _Previous) ->
  erlang:error(nif_not_loaded).

read_one_row(_Bin, _Depth, _Previous, _Scale) ->
  erlang:error(nif_not_loaded).


format_header_value(date, {Y, M, D}) ->
  io_lib:format("~4..0B-~2..0B-~2..0B", [Y, M, D]);

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
  [YS, MS, DS] = string:tokens(DateStr, "/-."),
  { erlang:list_to_integer(YS),
    erlang:list_to_integer(MS),
    erlang:list_to_integer(DS)};

parse_header_value(stock, StockStr) ->
  erlang:list_to_atom(StockStr);

parse_header_value(_, Value) ->
  Value.
