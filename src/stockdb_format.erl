%%% @doc stockdb_format: module that codes and decodes 
%%% actual data to/from binary representation.
%%% Format version: 2
%%% Here "changed" flags for delta fields are aggregated at
%%% packet start to improve code/decode performance by
%%% byte-aligning LEB128 parts

-module(stockdb_format).
-author({"Danil Zagoskin", z@gosk.in}).

-include_lib("eunit/include/eunit.hrl").
-include("../include/stockdb.hrl").

%-on_load(init_nif/0).
%-export([read_one_row/2, read_one_row/3, read_one_row/4]).

-export([encode_full_md/2, encode_full_md/3, decode_full_md/2]).
-export([encode_delta_md/2, encode_delta_md/3, decode_delta_md/2]).
-export([encode_trade/3, decode_trade/1]).
-export([format_header_value/2, parse_header_value/2]).

-export([decode_packet/2, decode_packet/4]).


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



%% @doc Encode full MD packet with given timestamp and (nested) bid/ask list
-spec encode_full_md(Timestamp::integer(), BidAsk::[{Price::integer(), Volume::integer()}]) -> iolist().
encode_full_md(Timestamp, BidAsk) when is_integer(Timestamp) andalso is_list(BidAsk) ->
  nested_foldl(fun append_full_PV/2, <<1:1, 0:1, Timestamp:62/integer>>, BidAsk);

%% @doc Accept #md{} and scale for high-level encoding
encode_full_md(#md{timestamp = Timestamp, bid = Bid, ask = Ask}, Scale) ->
  encode_full_md(Timestamp, [scale(Bid, Scale), scale(Ask, Scale)]).

%% @doc Alias for encode_full_md/2 with explicit Bid/Ask
encode_full_md(Timestamp, Bid, Ask) ->
  encode_full_md(Timestamp, [Bid, Ask]).

append_full_PV({Price, Volume}, Acc) when is_integer(Price) andalso is_integer(Volume) andalso Volume >= 0 ->
  <<Acc/binary, Price:32/signed-integer, Volume:32/unsigned-integer>>.


-spec decode_full_md(Buffer::binary(), Depth::integer()) ->
  {Timestamp::integer(), BidAsk::[{Price::integer(), Volume::integer()}], ByteCount::integer()}.
decode_full_md(<<1:1, Timestamp:63/integer, BidAskTail/binary>>, Depth) ->
  {Bid, AskTail} = decode_full_bidask(BidAskTail, Depth),
  {Ask, _Tail} = decode_full_bidask(AskTail, Depth),
  {Timestamp, Bid, Ask, 8 + 2*2*Depth*4}.

decode_full_bidask(Tail, 0) ->
  {[], Tail};
decode_full_bidask(<<Price:32/signed-integer, Volume:32/unsigned-integer, Tail/binary>>, Depth) ->
  Line = {Price, Volume},
  {TailLines, FinalTail} = decode_full_bidask(Tail, Depth - 1),
  {[Line | TailLines], FinalTail}.


%% @doc Encode delta MD packet with given timestamp delta and (nested) bid/ask delta list
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

%% @doc Alias for encode_delta_md/2 with explicit Bid/Ask
encode_delta_md(TimeDelta, DBid, DAsk) when is_integer(TimeDelta) andalso is_list(DBid) andalso is_list(DAsk) ->
  encode_delta_md(TimeDelta, [DBid, DAsk]);

%% @doc Accept new md, previous md, scale for high-level encoding
encode_delta_md(#md{} = MD, #md{} = PrevMD, Scale) ->
  #md{timestamp = TimeDelta, bid = DBid, ask = DAsk} = compute_delta(PrevMD, MD),
  encode_delta_md(TimeDelta, [scale(DBid, Scale), scale(DAsk, Scale)]).

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
  {TimeDelta::integer(),
    BidDelta::[{DPrice::integer(), DVolume::integer()}],
    AskDelta::[{DPrice::integer(), DVolume::integer()}], ByteCount::integer()}.
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
  {TimeDelta, DBid, DAsk, ByteCount}.

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


%% @doc Univeral decoding function: takes binary and depth, returns packet and its size
-spec decode_packet(Bin::binary(), Depth::integer()) -> {ok, Packet::term(), Size::integer()}|{error, Reason::term()}.

decode_packet(<<1:1, 0:1, _/bitstring>> = Bin, Depth) ->
  {Timestamp, Bid, Ask, Size} = decode_full_md(Bin, Depth),
  {ok, #md{timestamp = Timestamp, bid = Bid, ask = Ask}, Size};

decode_packet(<<0:1, _/bitstring>> = Bin, Depth) ->
  {TimeDelta, DBid, DAsk, Size} = decode_delta_md(Bin, Depth),
  {ok, {delta_md, TimeDelta, DBid, DAsk}, Size};

decode_packet(<<1:1, 1:1, _/bitstring>> = Bin, _Depth) ->
  {Timestamp, Price, Volume, Size} = decode_trade(Bin),
  {ok, #trade{timestamp = Timestamp, price = Price, volume = Volume}, Size};

decode_packet(<<Header:8/binary, _/bitstring>> = _BadBin, _Depth) ->
  {error, {cannot_parse, Header}}.


%% @doc Main decoding function: takes binary and depth, returns packet type, body and size
-spec decode_packet(Bin::binary(), Depth::integer(), PrevMD::term(), Scale::integer()) ->
  {ok, Packet::term(), Size::integer()} | {error, Reason::term()}.

decode_packet(<<1:1, 0:1, _/bitstring>> = Bin, Depth, _PrevMD, Scale) ->
  {Timestamp, Bid, Ask, Size} = decode_full_md(Bin, Depth),
  {ok, #md{timestamp = Timestamp, bid = unscale(Bid, Scale), ask = unscale(Ask, Scale)}, Size};

decode_packet(<<0:1, _/bitstring>> = Bin, Depth, #md{} = PrevMD, Scale) ->
  {TimeDelta, DBid, DAsk, Size} = decode_delta_md(Bin, Depth),
  Result = apply_delta(PrevMD, #md{timestamp = TimeDelta, bid = unscale(DBid, Scale), ask = unscale(DAsk, Scale)}),
  {ok, Result, Size};

decode_packet(<<1:1, 1:1, _/bitstring>> = Bin, _Depth, _PrevMD, Scale) ->
  {Timestamp, Price, Volume, Size} = decode_trade(Bin),
  {ok, #trade{timestamp = Timestamp, price = Price/Scale, volume = Volume}, Size};

decode_packet(<<Header:8/binary, _/bitstring>> = _BadBin, _Depth, _PrevMD, _Scale) ->
  {error, {cannot_parse, Header}}.


%% Utility: scale bid/ask when serializing
scale(BidAsk, Scale) when is_list(BidAsk) andalso is_integer(Scale) ->
  lists:map(fun({Price, Volume}) ->
        {erlang:round(Price*Scale), Volume}
    end, BidAsk).

%% Utility: unscale bid/ask when deserializing
unscale(BidAsk, Scale) when is_list(BidAsk) ->
  lists:map(fun({Price, Volume}) ->
        {Price/Scale, Volume}
    end, BidAsk).

%% Utility: apply delta md to previous md (actually, just sum field-by-field)
apply_delta(#md{timestamp = TS1, bid = B1, ask = A1}, #md{timestamp = TS2, bid = B2, ask = A2}) ->
  #md{timestamp = TS1 + TS2, bid = apply_delta(B1, B2), ask = apply_delta(A1, A2)};

apply_delta(BidAsk1, BidAsk2) when is_list(BidAsk1) andalso is_list(BidAsk2) ->
  lists:zipwith(fun({P1, V1}, {P2, V2}) ->
        {P1 + P2, V1 + V2}
    end, BidAsk1, BidAsk2).

%% Utility: get delta md where first argument is old value, second is new one
compute_delta(#md{timestamp = TS1, bid = B1, ask = A1}, #md{timestamp = TS2, bid = B2, ask = A2}) ->
  #md{timestamp = TS2 - TS1, bid = compute_delta(B1, B2), ask = compute_delta(A1, A2)};

compute_delta(BidAsk1, BidAsk2) when is_list(BidAsk1) andalso is_list(BidAsk2) ->
  % Count delta when going from BidAsk1 to BidAsk2 -> X = X2 - X1
  lists:zipwith(fun({P1, V1}, {P2, V2}) ->
        {P2 - P1, V2 - V1}
    end, BidAsk1, BidAsk2).


read_one_row(_Bin, _Depth) ->
  erlang:error(nif_not_loaded).

read_one_row(_Bin, _Depth, _Previous) ->
  erlang:error(nif_not_loaded).

read_one_row(_Bin, _Depth, _Previous, _Scale) ->
  erlang:error(nif_not_loaded).


%% @doc serialize header value, used when writing header
format_header_value(date, {Y, M, D}) ->
  io_lib:format("~4..0B-~2..0B-~2..0B", [Y, M, D]);

format_header_value(stock, Stock) ->
  erlang:atom_to_list(Stock);

format_header_value(_, Value) ->
  io_lib:print(Value).


%% @doc deserialize header value, used when parsing header
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
