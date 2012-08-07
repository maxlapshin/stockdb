-module(stockdb_format_tests).

% EUnit tests
-include_lib("eunit/include/eunit.hrl").

full_md_test() ->
  Timestamp = 16#138BDF77CBA,
  BidAsk = [[{1530, 250}, {1520, 111}], [{1673, 15}, {1700, 90}]],
  Bin = <<16#80000138BDF77CBA:64/integer,
    1530:32/integer, 250:32/integer,  1520:32/integer, 111:32/integer,
    1673:32/integer, 15:32/integer,   1700:32/integer, 90:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(full_md, stockdb_format:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format:encode_full_md(Timestamp, BidAsk)),
  ?assertEqual({Timestamp, BidAsk, Tail}, stockdb_format:decode_full_md(<<Bin/binary, Tail/bitstring>>, 2)).

full_md_negative_price_test() ->
  Timestamp = 16#138BDF77CBA,
  BidAsk = [[{-1530, 250}, {-1520, 111}], [{-1673, 15}, {-1700, 90}]],
  Bin = <<16#80000138BDF77CBA:64/integer,
    -1530:32/signed-integer, 250:32/integer,  -1520:32/signed-integer, 111:32/integer,
    -1673:32/signed-integer, 15:32/integer,   -1700:32/signed-integer, 90:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(full_md, stockdb_format:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format:encode_full_md(Timestamp, BidAsk)),
  ?assertEqual({Timestamp, BidAsk, Tail}, stockdb_format:decode_full_md(<<Bin/binary, Tail/bitstring>>, 2)).

full_md_negative_volume_test() ->
  Timestamp = 16#138BDF77CBA,
  BidAsk = [[{1530, 250}, {1520, 111}], [{1673, -15}, {1700, 90}]],
  ?assertException(error, function_clause, stockdb_format:encode_full_md(Timestamp, BidAsk)).


delta_md_test() ->
  DTimestamp = 270,
  DBidAsk = [[{0, -4}, {0, 0}], [{-230, 100}, {-100, -47}]],
  Bin = <<0:1, 142,2,  0:1, 1:1,124,  0:1, 0:1,  1:1,154,126, 1:1,228,0,  1:1,156,127,  1:1,81,  0:7>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(delta_md, stockdb_format:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format:encode_delta_md(DTimestamp, DBidAsk)),
  ?assertEqual({DTimestamp, DBidAsk, Tail}, stockdb_format:decode_delta_md(<<Bin/bitstring, Tail/bitstring>>, 2)).

trade_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = 16#DEAD,
  Volume = 16#BEEF,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/integer, Volume:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(trade, stockdb_format:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, Tail}, stockdb_format:decode_trade(<<Bin/binary, Tail/binary>>)).

trade_zerovolume_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = 16#DEAD,
  Volume = 0,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/integer, Volume:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(trade, stockdb_format:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, Tail}, stockdb_format:decode_trade(<<Bin/binary, Tail/binary>>)).

trade_negative_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = -16#DEAD,
  Volume = 16#BEEF,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/signed-integer, Volume:32/unsigned-integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(trade, stockdb_format:packet_type(Bin)),
  ?assertEqual(Bin, stockdb_format:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, Tail}, stockdb_format:decode_trade(<<Bin/binary, Tail/binary>>)),
  % Negative volume must fail to encode
  ?assertException(error, function_clause, stockdb_format:encode_trade(Timestamp, Price, -Volume)).

timestamp_test() ->
  ?assertEqual(16#138BDF77CBA, stockdb_format:decode_timestamp(<<16#80000138BDF77CBA:64/integer, 7, 239, 183, 19>>)),
  ?assertEqual(16#138BDF77CBA, stockdb_format:decode_timestamp(<<16#C0000138BDF77CBA:64/integer, 7, 239, 183, 19>>)).

format_header_value_test() ->
  ?assertEqual("2012/07/19", lists:flatten(stockdb_format:format_header_value(date, {2012, 7, 19}))),
  ?assertEqual("MICEX.URKA", lists:flatten(stockdb_format:format_header_value(stock, 'MICEX.URKA'))),
  ?assertEqual("17", lists:flatten(stockdb_format:format_header_value(depth, 17))),
  ?assertEqual("17", lists:flatten(stockdb_format:format_header_value(scale, 17))),
  ?assertEqual("17", lists:flatten(stockdb_format:format_header_value(chunk_size, 17))),
  ?assertEqual("17", lists:flatten(stockdb_format:format_header_value(version, 17))).

parse_header_value_test() ->
  ?assertEqual({2012, 7, 19}, stockdb_format:parse_header_value(date, "2012/07/19")),
  ?assertEqual('MICEX.URKA', stockdb_format:parse_header_value(stock, "MICEX.URKA")),
  ?assertEqual(17, stockdb_format:parse_header_value(depth, "17")),
  ?assertEqual(17, stockdb_format:parse_header_value(scale, "17")),
  ?assertEqual(17, stockdb_format:parse_header_value(chunk_size, "17")),
  ?assertEqual(17, stockdb_format:parse_header_value(version, "17")).


c_encode_full_md_test() ->
  Timestamp = 1343207118230, 
  Bid = [{1234, 715}, {1219, 201}, {1197, 1200}],
  Ask = [{1243, 601}, {1247, 1000}, {1260, 800}],
  Depth = length(Bid),
  Bin = stockdb_format:encode_full_md(Timestamp, Bid ++ Ask),
  ?assertEqual({ok, {md, Timestamp, Bid, Ask}, <<1,2,3,4>>}, stockdb_format:read_one_row(<<Bin/binary, 1,2,3,4>>, Depth)),
  ?assertEqual({Timestamp, [Bid, Ask], <<1,2,3,4>>}, stockdb_format:decode_full_md(<<Bin/binary, 1,2,3,4>>, Depth)),

  N = 100000,
  L = lists:seq(1,N),
  B1 = <<Bin/binary, 1,2,3,4>>,
  T1 = erlang:now(),
  [stockdb_format:read_one_row(B1, Depth) || _ <- L],
  T2 = erlang:now(),
  [stockdb_format:decode_full_md(B1, Depth) || _ <- L],
  T3 = erlang:now(),
  ?debugFmt("Full  ~p: nif ~B, erl ~B~n", [N, timer:now_diff(T2,T1), timer:now_diff(T3,T2)]),
  ok.

c_encode_delta_md_test() ->
  Timestamp = 15, 
  Bid = [{0, 5}, {-1, 20}, {4334, 1200}],
  Ask = [{12, 0}, {0, 0}, {1000, 800}],
  Depth = length(Bid),
  Bin = stockdb_format:encode_delta_md(Timestamp, Bid ++ Ask),
  ?assertEqual({ok, {delta, Timestamp, Bid, Ask}, <<1,2,3,4>>}, stockdb_format:read_one_row(<<Bin/binary, 1,2,3,4>>, Depth)),
  ?assertEqual({Timestamp, [Bid, Ask], <<1,2,3,4>>}, stockdb_format:decode_delta_md(<<Bin/binary, 1,2,3,4>>, Depth)),

  N = 100000,
  L = lists:seq(1,N),
  B1 = <<Bin/binary, 1,2,3,4>>,
  T1 = erlang:now(),
  [stockdb_format:read_one_row(B1, Depth) || _ <- L],
  T2 = erlang:now(),
  [stockdb_format:decode_delta_md(B1, Depth) || _ <- L],
  T3 = erlang:now(),
  ?debugFmt("Delta ~p: nif ~B, erl ~B~n", [N, timer:now_diff(T2,T1), timer:now_diff(T3,T2)]),
  ok.


c_encode_two_delta_md_test() ->
  Timestamp1 = 1343207118230, 
  Bid1 = [{1234, 715}, {1219, 201}, {1197, 1200}],
  Ask1 = [{1243, 601}, {1247, 1000}, {1260, 800}],
  Depth = length(Bid1),
  Bin1 = stockdb_format:encode_full_md(Timestamp1, Bid1 ++ Ask1),
  
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

  Bin2 = stockdb_format:encode_delta_md(Timestamp2, Bid2 ++ Ask2),
  
  ?assertEqual({ok, {md, Timestamp1, Bid1, Ask1}, Bin2}, stockdb_format:read_one_row(<<Bin1/binary, Bin2/binary>>, Depth)),
  MD = {md, Timestamp1, Bid1, Ask1},
  ?assertEqual({ok, {md, Timestamp2_, Bid2_, Ask2_}, <<>>}, stockdb_format:read_one_row(Bin2, Depth, MD)),
  
  % N = 100000,
  % L = lists:seq(1,N),
  % B1 = <<Bin/binary, 1,2,3,4>>,
  % T1 = erlang:now(),
  % [stockdb_format:read_one_row(B1, Depth) || _ <- L],
  % T2 = erlang:now(),
  % [stockdb_format:decode_delta_md(B1, Depth) || _ <- L],
  % T3 = erlang:now(),
  % ?debugFmt("Delta ~p: nif ~B, erl ~B~n", [N, timer:now_diff(T2,T1), timer:now_diff(T3,T2)]),
  ok.
