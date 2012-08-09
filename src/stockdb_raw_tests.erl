-module(stockdb_raw_tests).
-include("stockdb_test_content.hrl").

-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).



db_repair_test() ->
  File = ?TEMPFILE("db-repair-test.temp"),
  write_events_to_file(File, chunk_109_content() ++ chunk_110_content_1()),

  {ok, F} = file:open(File, [read, write]),
  {ok, _} = file:position(F, {eof, -1}),
  ok = file:truncate(F),
  ok = file:close(F),

  {ok, S1} = stockdb_raw:open(File, [read]),
  ?assertThrow({truncate_failed, _}, stockdb_raw:restore_state(S1)),

  append_events_to_file(File, chunk_110_content_2() ++ chunk_112_content()),

  {ok, FileEvents} = stockdb_raw:read_file(File),
  lists:zipwith(fun(Expected, Read) ->
        ensure_packets_equal(Expected, Read)
    end,
    chunk_109_content() ++ chunk_110_content_1_trunc() ++ chunk_110_content_2() ++ chunk_112_content(),
    FileEvents),

  ok = file:delete(File).



foldl_test() ->
  File = ?TEMPFILE("foldl-test.temp"),
  write_events_to_file(File, full_content()),

  % Meaningless functions. We know that events are stored correctly,
  % so just test folding
  CountFun = fun(_, Count) -> Count+1 end,

  FoldFun2 = fun
    ({md, _UTC, Bid, Ask}, AccIn) ->
      AccIn + length(Bid) + length(Ask);
    ({trade, _UTC, Price, _Volume}, AccIn) ->
      AccIn - erlang:round(Price)
  end,

  ?assertEqual(lists:foldl(CountFun, 0, full_content()),
    stockdb_raw:foldl(CountFun, 0, File)),

  ?assertEqual(lists:foldl(FoldFun2, 720, full_content()),
    stockdb_raw:foldl(FoldFun2, 720, File)),

  ok = file:delete(File).

foldl_range_test() ->
  File = ?TEMPFILE("foldl-range-test.temp"),
  write_events_to_file(File, full_content()),

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
  Events1 = chunk_109_content() ++ chunk_110_content_1(),

  Range2 = {1343207500000, undefined},
  Events2 = chunk_110_content_2() ++ chunk_112_content(),

  ?assertEqual(lists:foldl(CountFun, 0, Events1),
    stockdb_raw:foldl_range(CountFun, 0, File, Range1)),

  ?assertEqual(lists:foldl(FoldFun2, 720, Events2),
    stockdb_raw:foldl_range(FoldFun2, 720, File, Range2)),

  ok = file:delete(File).
