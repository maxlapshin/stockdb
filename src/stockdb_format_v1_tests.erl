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


