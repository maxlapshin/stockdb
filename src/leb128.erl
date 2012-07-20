%%% @doc LEB128 (http://en.wikipedia.org/wiki/LEB128) realization
%%% Only unsigned integers re supported ATM

-module(leb128).
-author({"Danil Zagoskin", z@gosk.in}).

-export([encode/1, decode/1]).
-export([profile/0]).

-spec encode(integer()) -> binary().
encode(Value) when is_integer(Value) ->
  encode_unsigned(Value).

encode_unsigned(Value) ->
  case Value bsr 7 of
    0 ->
      <<0:1, Value:7/integer>>;
    NextValue ->
      <<1:1, Value:7/integer, (encode_unsigned(NextValue))/binary>>
  end.


encode1(Value) when is_integer(Value) andalso Value >= 0 andalso Value < 128 ->
  <<0:1, Value:7/integer>>;

encode1(Value) when is_integer(Value) andalso Value >= 128 andalso Value < 16384 ->
  <<1:1, Value:7/integer, 0:1, (Value bsr 7):7/integer>>;

encode1(Value) when is_integer(Value) andalso Value >= 16384 andalso Value < 2097152 ->
  <<1:1, Value:7/integer, 1:1, (Value bsr 7):7/integer, 0:1, (Value bsr 14):7/integer>>;

encode1(Value) when is_integer(Value) andalso Value >= 2097152 andalso Value < 268435456 ->
  <<1:1, Value:7/integer, 1:1, (Value bsr 7):7/integer, 1:1, (Value bsr 14):7/integer, 0:1, (Value bsr 21):7/integer>>;

encode1(Value) when is_integer(Value) andalso Value >= 268435456 andalso Value < 34359738368 ->
  <<1:1, Value:7/integer, 1:1, (Value bsr 7):7/integer, 1:1, (Value bsr 14):7/integer, 1:1, (Value bsr 21):7/integer, 0:1, (Value bsr 28):7/integer>>.




-spec decode(binary()) -> {integer(), binary()}.
decode(Bin) when is_bitstring(Bin) ->
  decode_chunks(Bin, []).

decode_chunks(<<0:1, Chunk:7/integer, Tail/bitstring>>, Acc) ->
  Result = lists:foldl(fun(V, Sum) ->
    (Sum bsl 7) bor V
  end, 0, [Chunk|Acc]),
  {Result, Tail};
  
decode_chunks(<<1:1, Chunk:7/integer, Tail/bitstring>>, Acc) ->
  decode_chunks(Tail, [Chunk|Acc]).


decode1(<<0:1, V0:7, Tail/bitstring>>) ->
  {V0, Tail};

decode1(<<1:1, V0:7, 0:1, V1:7, Tail/bitstring>>) ->
  {(V1 bsl 7) bor V0, Tail};

decode1(<<1:1, V0:7, 1:1, V1:7, 0:1, V2:7, Tail/bitstring>>) ->
  {(V2 bsl 14) bor (V1 bsl 7) bor V0, Tail};

decode1(<<1:1, V0:7, 1:1, V1:7, 1:1, V2:7, 0:1, V3:7, Tail/bitstring>>) ->
  {(V3 bsl 21) bor (V2 bsl 14) bor (V1 bsl 7) bor V0, Tail};

decode1(<<1:1, V0:7, 1:1, V1:7, 1:1, V2:7, 1:1, V3:7, 0:1, V4:7, Tail/bitstring>>) ->
  {(V4 bsl 21) bor (V3 bsl 21) bor (V2 bsl 14) bor (V1 bsl 7) bor V0, Tail}.


% EUnit tests
-include_lib("eunit/include/eunit.hrl").

% Wikipedia example
encode_test() ->
  ?assertEqual(<<16#E5, 16#8E, 16#26>>, encode(624485)).

encode1_test() ->
  ?assertEqual(<<16#E5, 16#8E, 16#26>>, encode1(624485)).

decode_test() ->
  ?assertEqual({624485,<<57,113,1:4>>}, decode(<<16#E5, 16#8E, 16#26, 57,113,1:4>>)).

decode1_test() ->
  ?assertEqual({624485,<<57,113,1:4>>}, decode1(<<16#E5, 16#8E, 16#26, 57,113,1:4>>)).


measure(F) ->
  T1 = erlang:now(),
  Res = F(),
  T2 = erlang:now(),
  {timer:now_diff(T2,T1), Res}.

profile() ->
  Count = 1000000,
  List0 = [A || <<A:32>> <= crypto:rand_bytes(Count*3)],
  {Delta1, List1_} = measure(fun() -> [encode(A) || A <- List0] end),
  
  List1 = [<<A/binary, (crypto:rand_bytes(40))/binary>> || A <- List1_],

  {Delta2, List2} = measure(fun() -> [decode(B) || B <- List1] end),

  {Delta3, List3} = measure(fun() -> [decode1(B) || B <- List1] end),

  {Delta4, List4} = measure(fun() -> [encode1(A) || A <- List0] end),
  
  List0 = [A || {A,_} <- List2],
  % List1 = [A || {A,<<>>} <- List4],
  
  {enc, Delta1 / Count, dec, Delta2 / Count, dec1, Delta3 / Count, enc1, Delta4 / Count}.
