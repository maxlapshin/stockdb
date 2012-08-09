-module(stockdb_raw_tests).

-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).


-import(stockdb_test_helper, [tempfile/1, write_events_to_file/2, append_events_to_file/2, ensure_packets_equal/2, chunk_content/1]).




foldl_test() ->
  File = tempfile("foldl-test.temp"),
  write_events_to_file(File, chunk_content('full')),

  % Meaningless functions. We know that events are stored correctly,
  % so just test folding
  CountFun = fun(_, Count) -> Count+1 end,

  FoldFun2 = fun
    ({md, _UTC, Bid, Ask}, AccIn) ->
      AccIn + length(Bid) + length(Ask);
    ({trade, _UTC, Price, _Volume}, AccIn) ->
      AccIn - erlang:round(Price)
  end,

  ?assertEqual(lists:foldl(CountFun, 0, chunk_content('full')),
    stockdb_raw:foldl(CountFun, 0, File)),

  ?assertEqual(lists:foldl(FoldFun2, 720, chunk_content('full')),
    stockdb_raw:foldl(FoldFun2, 720, File)),

  ok = file:delete(File).

foldl_range_test() ->
  File = tempfile("foldl-range-test.temp"),
  write_events_to_file(File, chunk_content('full')),

  % Meaningless functions. We know that events are stored correctly,
  % so just test folding
  CountFun = fun(_, Count) -> Count+1 end,

  FoldFun2 = fun
    ({md, _UTC, Bid, Ask}, AccIn) ->
      AccIn + length(Bid) + length(Ask);
    ({trade, _UTC, Price, _Volume}, AccIn) ->
      AccIn - erlang:round(Price)
  end,

  Range1 = {undefined, 1343207500000},
  Events1 = chunk_content('109') ++ chunk_content('110_1'),

  Range2 = {1343207500000, undefined},
  Events2 = chunk_content('110_2') ++ chunk_content('112'),

  ?assertEqual(lists:foldl(CountFun, 0, Events1),
    stockdb_raw:foldl_range(CountFun, 0, File, Range1)),

  ?assertEqual(lists:foldl(FoldFun2, 720, Events2),
    stockdb_raw:foldl_range(FoldFun2, 720, File, Range2)),

  ok = file:delete(File).
