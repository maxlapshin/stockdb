%%% @doc LEB128 (http://en.wikipedia.org/wiki/LEB128) realization
%%% Only unsigned integers re supported ATM

-module(leb128).
-author({"Danil Zagoskin", z@gosk.in}).

-export([encode/1, decode/1]).
-export([encode_signed/1, decode_signed/1]).
-export([profile/0]).


-spec encode_signed(integer()) -> bitstring().
encode_signed(Value) when is_integer(Value)  ->
  encode_ranged(Value, 0, 16#40).

-spec encode(non_neg_integer()) -> binary().
encode(Value) when is_integer(Value) andalso Value >= 0 ->
  encode_ranged(Value, 0, 16#80).


encode_ranged(Value, Shift, Range) when -Range =< Value andalso Value < Range ->
  Chunk = Value bsr Shift,
  <<0:1, Chunk:7/integer>>;

encode_ranged(Value, Shift, Range) ->
  Chunk = Value bsr Shift,
  Tail = encode_ranged(Value, Shift+7, Range bsl 7),
  <<1:1, Chunk:7/integer, Tail/binary>>.


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
  decode(Bin, unsigned).

-spec decode_signed(bitstring()) -> integer().
decode_signed(Bin) ->
  decode(Bin, signed).

decode(Bin, signed) ->
  {Size, UValue, Tail} = take_chunks(Bin, 0, 0),
  <<Value:Size/signed-integer>> = <<UValue:Size/unsigned-integer>>,
  {Value, Tail};

decode(Bin, unsigned) ->
  {_Size, Value, Tail} = take_chunks(Bin, 0, 0),
  {Value, Tail}.

take_chunks(<<0:1, Chunk:7/integer, Tail/bitstring>>, Acc, Shift) ->
  Result = (Chunk bsl Shift) bor Acc,
  {Shift+7, Result, Tail};

take_chunks(<<1:1, Chunk:7/integer, Tail/bitstring>>, Acc, Shift) ->
  NextAcc = (Chunk bsl Shift) bor Acc,
  take_chunks(Tail, NextAcc, Shift+7).


% EUnit tests
-include_lib("eunit/include/eunit.hrl").

% Wikipedia example
encode_test() ->
  ?assertEqual(<<16#E5, 16#8E, 16#26>>, encode(624485)).

encode1_test() ->
  ?assertEqual(<<16#E5, 16#8E, 16#26>>, encode1(624485)).

decode_test() ->
  ?assertEqual({624485,<<57,113,1:4>>}, decode(<<16#E5, 16#8E, 16#26, 57,113,1:4>>)).

encode_signed_test() ->
  ?assertEqual(<<16#9B, 16#F1, 16#59>>, encode_signed(-624485)).

decode_signed_test() ->
  ?assertEqual({-624485,<<57,113,1:4>>}, decode_signed(<<16#9B, 16#F1, 16#59, 57,113,1:4>>)).


measure(F) ->
  T1 = erlang:now(),
  Res = F(),
  T2 = erlang:now(),
  {timer:now_diff(T2,T1), Res}.

profile() ->
  Count = 100000,
  List0 = [A || <<A:32>> <= crypto:rand_bytes(Count*3)],
  {DeltaE, List1_} = measure(fun() -> [encode(A) || A <- List0] end),
  {DeltaE1, List1_} = measure(fun() -> [encode1(A) || A <- List0] end),
  
  List1 = [<<A/binary, (crypto:rand_bytes(40))/binary>> || A <- List1_],

  {DeltaD, List2} = measure(fun() -> [decode(B) || B <- List1] end),
  
  List0 = [A || {A,_} <- List2],
  % List1 = [A || {A,<<>>} <- List4],
  
  {enc, [DeltaE / Count, DeltaE1 / Count],  dec, [DeltaD / Count]}.
