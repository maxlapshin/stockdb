-module(stockdb_format_v1_tests).

% EUnit tests
-include_lib("eunit/include/eunit.hrl").

full_md_test() ->
  Timestamp = 16#138BDF77CBA,
  BidAsk = [[{1530, 250}, {1520, 111}], [{1673, 15}, {1700, 90}]],
  Bin = <<16#80000138BDF77CBA:64/integer,
    1530:32/integer, 250:32/integer,  1520:32/integer, 111:32/integer,
    1673:32/integer, 15:32/integer,   1700:32/integer, 90:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(full_md, stockdb_format_v1:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format_v1:encode_full_md(Timestamp, BidAsk)),
  ?assertEqual({Timestamp, BidAsk, Tail}, stockdb_format_v1:decode_full_md(<<Bin/binary, Tail/bitstring>>, 2)).

full_md_negative_price_test() ->
  Timestamp = 16#138BDF77CBA,
  BidAsk = [[{-1530, 250}, {-1520, 111}], [{-1673, 15}, {-1700, 90}]],
  Bin = <<16#80000138BDF77CBA:64/integer,
    -1530:32/signed-integer, 250:32/integer,  -1520:32/signed-integer, 111:32/integer,
    -1673:32/signed-integer, 15:32/integer,   -1700:32/signed-integer, 90:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(full_md, stockdb_format_v1:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format_v1:encode_full_md(Timestamp, BidAsk)),
  ?assertEqual({Timestamp, BidAsk, Tail}, stockdb_format_v1:decode_full_md(<<Bin/binary, Tail/bitstring>>, 2)).

full_md_negative_volume_test() ->
  Timestamp = 16#138BDF77CBA,
  BidAsk = [[{1530, 250}, {1520, 111}], [{1673, -15}, {1700, 90}]],
  ?assertException(error, function_clause, stockdb_format_v1:encode_full_md(Timestamp, BidAsk)).


delta_md_test() ->
  DTimestamp = 270,
  DBidAsk = [[{0, -4}, {0, 0}], [{-230, 100}, {-100, -47}]],
  Bin = <<0:1, 142,2,  0:1, 1:1,124,  0:1, 0:1,  1:1,154,126, 1:1,228,0,  1:1,156,127,  1:1,81,  0:7>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(delta_md, stockdb_format_v1:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format_v1:encode_delta_md(DTimestamp, DBidAsk)),
  ?assertEqual({DTimestamp, DBidAsk, Tail}, stockdb_format_v1:decode_delta_md(<<Bin/bitstring, Tail/bitstring>>, 2)).

trade_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = 16#DEAD,
  Volume = 16#BEEF,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/integer, Volume:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(trade, stockdb_format_v1:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format_v1:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, Tail}, stockdb_format_v1:decode_trade(<<Bin/binary, Tail/binary>>)).

trade_zerovolume_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = 16#DEAD,
  Volume = 0,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/integer, Volume:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(trade, stockdb_format_v1:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format_v1:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, Tail}, stockdb_format_v1:decode_trade(<<Bin/binary, Tail/binary>>)).

trade_negative_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = -16#DEAD,
  Volume = 16#BEEF,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/signed-integer, Volume:32/unsigned-integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(trade, stockdb_format_v1:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format_v1:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, Tail}, stockdb_format_v1:decode_trade(<<Bin/binary, Tail/binary>>)),
  % Negative volume must fail to encode
  ?assertException(error, function_clause, stockdb_format_v1:encode_trade(Timestamp, Price, -Volume)).


format_header_value_test() ->
  ?assertEqual("2012/07/19", lists:flatten(stockdb_format_v1:format_header_value(date, {2012, 7, 19}))),
  ?assertEqual("MICEX.URKA", lists:flatten(stockdb_format_v1:format_header_value(stock, 'MICEX.URKA'))),
  ?assertEqual("17", lists:flatten(stockdb_format_v1:format_header_value(depth, 17))),
  ?assertEqual("17", lists:flatten(stockdb_format_v1:format_header_value(scale, 17))),
  ?assertEqual("17", lists:flatten(stockdb_format_v1:format_header_value(chunk_size, 17))),
  ?assertEqual("17", lists:flatten(stockdb_format_v1:format_header_value(version, 17))).

parse_header_value_test() ->
  ?assertEqual({2012, 7, 19}, stockdb_format_v1:parse_header_value(date, "2012/07/19")),
  ?assertEqual('MICEX.URKA', stockdb_format_v1:parse_header_value(stock, "MICEX.URKA")),
  ?assertEqual(17, stockdb_format_v1:parse_header_value(depth, "17")),
  ?assertEqual(17, stockdb_format_v1:parse_header_value(scale, "17")),
  ?assertEqual(17, stockdb_format_v1:parse_header_value(chunk_size, "17")),
  ?assertEqual(17, stockdb_format_v1:parse_header_value(version, "17")).



c_decode_full_md_with_scale_test() ->
  Timestamp = 1343207118230, 
  Bid = [{1234, 715}, {1219, 201}, {1197, 1200}],
  Ask = [{1243, 601}, {1247, 1000}, {1260, 800}],
  % Timestamp = 1, 
  % Bid = [{12, 15}],
  % Ask = [{12, 1}],
  Depth = length(Bid),
  Scale = 100,
  Bin = stockdb_format_v1:encode_full_md(Timestamp, Bid ++ Ask),
  ?assertMatch({ok, {md, Timestamp, _, _}, <<>>}, stockdb_format_v1:read_one_row(Bin, Depth, undefined, Scale)),
  {ok, {md, Timestamp, Bid1, Ask1}, <<>>} = stockdb_format_v1:read_one_row(Bin, Depth, undefined, Scale),
  ?assertEqual(Bid, [{round(P*Scale),V} || {P,V} <- Bid1]),
  ?assertEqual(Ask, [{round(P*Scale),V} || {P,V} <- Ask1]),
  ensure_error_on_shorter_bin(fun(B) -> stockdb_format_v1:read_one_row(B, Depth, undefined, Scale) end, Bin),
  ok.
  

c_decode_full_md_test() ->
  Timestamp = 1343207118230, 
  Bid = [{1234, 715}, {1219, 201}, {1197, 1200}],
  Ask = [{1243, 601}, {1247, 1000}, {1260, 800}],
  Depth = length(Bid),
  Bin = stockdb_format_v1:encode_full_md(Timestamp, Bid ++ Ask),
  ?assertEqual({ok, {md, Timestamp, Bid, Ask}, <<1,2,3,4>>}, stockdb_format_v1:read_one_row(<<Bin/binary, 1,2,3,4>>, Depth)),
  ?assertEqual({Timestamp, [Bid, Ask], <<1,2,3,4>>}, stockdb_format_v1:decode_full_md(<<Bin/binary, 1,2,3,4>>, Depth)),
  ensure_error_on_shorter_bin(fun(B) -> stockdb_format_v1:read_one_row(B, Depth) end, Bin),

  N = 100000,
  L = lists:seq(1,N),
  B1 = <<Bin/binary, 1,2,3,4>>,
  T1 = erlang:now(),
  [stockdb_format_v1:read_one_row(B1, Depth) || _ <- L],
  T2 = erlang:now(),
  [stockdb_format_v1:decode_full_md(B1, Depth) || _ <- L],
  T3 = erlang:now(),
  ?debugFmt("Full  ~p: nif ~B, erl ~B", [N, timer:now_diff(T2,T1), timer:now_diff(T3,T2)]),
  ok.

c_decode_trade_test() ->
  Timestamp = 1343207118230,
  Price = 1234,
  Volume = 1000,
  Bin = stockdb_format_v1:encode_trade(Timestamp, Price, Volume),
  ?assertEqual({ok, {trade, Timestamp, Price, Volume}, <<>>}, stockdb_format_v1:read_one_row(Bin, 0)),
  ?assertEqual({ok, {trade, Timestamp, 12.34, Volume}, <<>>}, stockdb_format_v1:read_one_row(Bin, 0, undefined, 100)),
  ensure_error_on_shorter_bin(fun(B) -> stockdb_format_v1:read_one_row(B, 0, undefined, 100) end, Bin),
  ok.

c_decode_delta_md_test() ->
  Timestamp = 15, 
  Bid = [{0, 5}, {-1, 20}, {4334, 1200}],
  Ask = [{12, 0}, {0, 0}, {1000, 800}],
  Depth = length(Bid),
  Bin = stockdb_format_v1:encode_delta_md(Timestamp, Bid ++ Ask),
  ?assertEqual({ok, {delta, Timestamp, Bid, Ask}, <<1,2,3,4>>}, stockdb_format_v1:read_one_row(<<Bin/binary, 1,2,3,4>>, Depth)),
  ?assertEqual({Timestamp, [Bid, Ask], <<1,2,3,4>>}, stockdb_format_v1:decode_delta_md(<<Bin/binary, 1,2,3,4>>, Depth)),
  ensure_error_on_shorter_bin(fun(B) -> stockdb_format_v1:read_one_row(B, Depth) end, Bin),

  N = 100000,
  L = lists:seq(1,N),
  B1 = <<Bin/binary, 1,2,3,4>>,
  T1 = erlang:now(),
  [stockdb_format_v1:read_one_row(B1, Depth) || _ <- L],
  T2 = erlang:now(),
  [stockdb_format_v1:decode_delta_md(B1, Depth) || _ <- L],
  T3 = erlang:now(),
  ?debugFmt("Delta ~p: nif ~B, erl ~B", [N, timer:now_diff(T2,T1), timer:now_diff(T3,T2)]),
  ok.


c_decode_multi_test() ->
  Data = <<128,0,1,56,189,98,21,150,0,0,9,164,0,0,2,203,0,0,9,135,0,0,0,201,0,0,9,90,0,
    0,4,176,0,0,9,183,0,0,2,89,0,0,9,190,0,0,3,232,0,0,9,216,0,0,3,32,114,76,1,
    53,47,215,225,1,9,144,75,104,1,58,224,48,35,88,5,127,65,16,160,111,82,129,
    131,168,250,241,128,95,23,108,51,240,192,0,1,56,189,100,115,75,0,0,9,162,0,0,
    1,234,100,198,0,188,47,141,123,248,242,0,16,160,65,64,0,223,62,128,119,188,
    192,19,56,255,1,60,128,16,128,102,73,128,193,49,143,144,95,67,235,200,1,127,
    51,143,247,128,192,0,1,56,189,101,208,159,0,0,9,184,0,0,0,200,84,77,0,128,
    245,63,41,192,192,0,1,56,189,102,107,246,0,0,9,185,0,0,1,44,128,0,1,56,189,
    102,138,148,0,0,9,166,0,0,3,32,0,0,9,150,0,0,1,194,0,0,9,90,0,0,4,176,0,0,9,
    183,0,0,1,194,0,0,9,190,0,0,3,82,0,0,9,226,0,0,2,88,104,157,129,3,239,1,65,
    50,192,32,86,200,5,179,143,247,250,115,56,255,126,209,62,223,185,0,46,64,8,
    79,194,129,95,188,128,16,28,227,251,200,1,2,239,1,64,183,15,205,195,240,192,
    0,1,56,189,106,60,134,0,0,9,186,0,0,0,200,128,0,1,56,189,113,17,178,0,0,9,
    166,0,0,2,188,0,0,9,149,0,0,1,144,0,0,9,90,0,0,4,76,0,0,9,184,0,0,1,194,0,0,
    9,192,0,0,4,176,0,0,9,226,0,0,3,32>>,
  
  MD1 = {md, 1343207118230, [{12.34, 715}, {12.195, 201}, {11.97, 1200}], [{12.435, 601}, {12.47, 1000}, {12.60, 800}]},
  MD2 = {md, 1343207154170, [{12.34, 500}, {12.185, 201}, {11.97, 1200}], [{12.440, 601}, {12.47, 1000}, {12.60, 850}]},
  MD3 = {md, 1343207197200, [{12.34, 715}, {12.195, 201}, {11.97, 1500}], [{12.435, 601}, {12.49, 1000}, {12.65, 850}]},
  Scale = 200,
  Depth = 3,
  {ok, MD1_, L1} = stockdb_format_v1:read_one_row(Data, Depth, undefined, Scale),
  {ok, MD2_, L2} = stockdb_format_v1:read_one_row(L1, Depth, MD1_, Scale),
  {ok, MD3_, _L3} = stockdb_format_v1:read_one_row(L2, Depth, MD2_, Scale),
  ?assertEqual(MD1,MD1_),
  ?assertEqual(MD2,MD2_),
  ?assertEqual(MD3,MD3_),
  ok.
  

c_decode_two_delta_md_test() ->
  Timestamp1 = 1343207118230, 
  Bid1 = [{1234, 715}, {1219, 201}, {1197, 1200}],
  Ask1 = [{1243, 601}, {1247, 1000}, {1260, 800}],
  Depth = length(Bid1),
  Bin1 = stockdb_format_v1:encode_full_md(Timestamp1, Bid1 ++ Ask1),
  
  Timestamp2 = 15, 
  Bid2 = [{0, 5}, {-1, 20}, {4334, 1200}],
  Ask2 = [{12, 0}, {0, 0}, {1000, 800}],
  
  Timestamp2_ = Timestamp1+Timestamp2,
  Bid2_ = lists:zipwith(fun({Price, Volume}, {DPrice, DVolume}) ->
    {Price + DPrice, Volume + DVolume}
  end, Bid1, Bid2),
  Ask2_ = lists:zipwith(fun({Price, Volume}, {DPrice, DVolume}) ->
    {Price + DPrice, Volume + DVolume}
  end, Ask1, Ask2),

  Bin2 = stockdb_format_v1:encode_delta_md(Timestamp2, Bid2 ++ Ask2),
  
  Bin = <<Bin1/binary, Bin2/binary>>,
  % DBState = stockdb_raw:init_with_opts([{depth,Depth},{buffer,Bin}]),
  
  ?assertEqual({ok, {md, Timestamp1, Bid1, Ask1}, Bin2}, stockdb_format_v1:read_one_row(Bin, Depth)),
  % {Event1,State2} = stockdb_raw:read_packet_from_buffer(DBState),
  % ?assertEqual({md,Timestamp1,Bid1,Ask1}, Event1),
  MD = {md, Timestamp1, Bid1, Ask1},
  ?assertEqual({ok, {md, Timestamp2_, Bid2_, Ask2_}, <<>>}, stockdb_format_v1:read_one_row(Bin2, Depth, MD)),

  % {Event2,_} = stockdb_raw:read_packet_from_buffer(State2),
  % ?assertEqual({md,Timestamp2_,Bid2_,Ask2_}, Event2),

  
  N = 100000,
  L = lists:seq(1,N),
  T1 = erlang:now(),
  [begin
    {ok, {md,_,_,_} = MD, Bin2} = stockdb_format_v1:read_one_row(Bin, Depth),
    {ok, {md,_,_,_}, <<>>} = stockdb_format_v1:read_one_row(Bin2,Depth, MD)
  end || _ <- L],
  T2 = erlang:now(),
  % [begin
  %   {{md,_,_,_},State1} = stockdb_raw:read_packet_from_buffer(DBState),
  %   {{md,_,_,_},_State2} = stockdb_raw:read_packet_from_buffer(State1)
  % end || _ <- L],
  T3 = erlang:now(),
  ?debugFmt("Two delta ~p: nif ~B, erl ~B", [N, timer:now_diff(T2,T1), timer:now_diff(T3,T2)]),
  ok.

ensure_error_on_shorter_bin(_Fun, <<>>) ->
  ok;
ensure_error_on_shorter_bin(Fun, Bin) ->
  ShorterLen = erlang:byte_size(Bin) - 1,
  <<ShorterBin:ShorterLen/binary, _:1/binary>> = Bin,
  ?assertMatch({{error, _}, _}, {Fun(ShorterBin), ShorterBin}),
  ensure_error_on_shorter_bin(Fun, ShorterBin).
