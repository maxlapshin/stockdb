%%% @doc stockdb_format: module that codes and decodes 
%%% actual data to/from binary representation.
%%% Format version: 2
%%% Here "changed" flags for delta fields are aggregated at
%%% packet start to improve code/decode performance by
%%% byte-aligning LEB128 parts

-module(stockdb_format).
-author({"Danil Zagoskin", z@gosk.in}).
-extends(stockdb_format_v1).

-on_load(init_nif/0).

%-export([read_one_row/2, read_one_row/3, read_one_row/4]).

-export([encode_full_md/2, decode_full_md/2]).
%-export([encode_delta_md/2, decode_delta_md/2]).
-export([encode_trade/3, decode_trade/1]).
-export([format_header_value/2, parse_header_value/2]).


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


-spec encode_trade(Timestamp::integer(), Price::integer(), Volume::integer()) -> iolist().
encode_trade(Timestamp, Price, Volume) when is_integer(Price) andalso is_integer(Volume) andalso Volume >= 0 ->
  <<1:1, 1:1, Timestamp:62/integer, Price:32/signed-integer, Volume:32/unsigned-integer>>.

-spec decode_trade(Buffer::binary()) -> {Timestamp::integer(), Price::integer(), Volume::integer(), ByteCount::integer()}.
decode_trade(<<1:1, 1:1, Timestamp:62/integer, Price:32/signed-integer, Volume:32/unsigned-integer, _/binary>>) ->
  {Timestamp, Price, Volume, 16}.


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
