%%% @doc Binary row format for stockdb
-module(stockdb_format).
-author({"Danil Zagoskin", z@gosk.in}).

-export([encode_full_md/2, encode_delta_md/2]).

nested_foldl(Fun, Acc0, List) when is_list(List) ->
  lists:foldl(fun(E, Acc) -> nested_foldl(Fun, Acc, E) end, Acc0, List);
nested_foldl(Fun, Acc, Element) ->
  Fun(Element, Acc).

-spec encode_full_md(Timestamp::integer(), BidAsk::[{Price::integer(), Volume::integer()}]) -> iolist().
encode_full_md(Timestamp, BidAsk) ->
  nested_foldl(fun({Price, Volume}, Acc) ->
        <<Acc/binary, Price:32/integer, Volume:32/integer>>
    end, <<1:1, Timestamp:63/integer>>, BidAsk).

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

encode_delta_value(0) -> <<0:1>>;
encode_delta_value(V) -> <<1:1, (leb128:encode_signed(V))/bitstring>>.
