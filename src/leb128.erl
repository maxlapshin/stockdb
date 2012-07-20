%%% @doc LEB128 (http://en.wikipedia.org/wiki/LEB128) realization
%%% Only unsigned integers re supported ATM

-module(leb128).
-author({"Danil Zagoskin", z@gosk.in}).

-export([encode/1, decode/1]).

-spec encode(integer()) -> binary().
encode(Value) when is_integer(Value) ->
  encode_unsigned(Value, <<>>).

encode_unsigned(Value, Acc) ->
  Chunk = <<Value:7/integer>>,
  case Value bsr 7 of
    0 ->
      <<Acc/binary, 0:1, Chunk/bitstring>>;
    NextValue ->
      encode_unsigned(NextValue, <<Acc/binary, 1:1, Chunk/bitstring>>)
  end.


-spec decode(binary()) -> {integer(), binary()}.
decode(Bin) when is_bitstring(Bin) ->
  {Chunks, Tail} = decode_chunks([], Bin),
  Result = lists:foldl(fun(Chunk, Acc) ->
        (Acc bsl 7) bor Chunk
    end, 0, Chunks),
  {Result, Tail}.

decode_chunks(Acc, <<0:1, Chunk:7/integer, Tail/bitstring>>) ->
  {[Chunk|Acc], Tail};
decode_chunks(Acc, <<1:1, Chunk:7/integer, Tail/bitstring>>) ->
  decode_chunks([Chunk|Acc], Tail).


% EUnit tests
-include_lib("eunit/include/eunit.hrl").

encode_test() ->
  % Wikipedia example
  ?assertEqual(<<16#E5, 16#8E, 16#26>>, encode(624485)).

decode_test() ->
  ?assertEqual({624485,<<57,113,1:4>>}, decode(<<16#E5, 16#8E, 16#26, 57,113,1:4>>)).
