-module(stockdb_raw_tests).
-include("stockdb_test_content.hrl").

-include_lib("eunit/include/eunit.hrl").

file_create_test() ->
  file_create_test([]).
raw_file_create_test() ->
  file_create_test([raw]).

file_create_test(Modes) ->
  check_creation_params(Modes ++ [{stock, 'TEST'}, {date, {2012,7,26}}, {depth, 10}, {scale, 100}, {chunk_size, 300}],
    "TEST-20120726.300.10.100.stock"),
  check_creation_params(Modes ++ [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 15}, {scale, 200}, {chunk_size, 600}],
    "TEST-20120725.600.15.200.stock").

check_creation_params(DBOptions, FixtureFile) ->
  ?assert(filelib:is_dir(?TESTDIR)),

  File = ?TEMPFILE("creation-test.temp"),
  ok = filelib:ensure_dir(File),
  ok = file:write_file(File, "GARBAGE"),

  {ok, S} = stockdb_raw:open(File, [write|DBOptions]),
  ok = stockdb_raw:close(S),
  db_no_regress(?FIXTUREFILE(FixtureFile), File),
  ok = file:delete(File).

db_no_regress(OldFile, NewFile) ->
  % TODO: Make something intelligent
  ?assertEqual(file:read_file(OldFile), file:read_file(NewFile)).

write_append_test() ->
  write_append_test([]).
raw_write_append_test() ->
  write_append_test([raw]).

write_append_test(Options) ->
  File = ?TEMPFILE("write-append-test.temp"),
  ok = filelib:ensure_dir(File),

  {ok, S0} = stockdb_raw:open(File, Options ++ [write, {stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_raw:append(Event, State),
        NextState
    end, S0, chunk_109_content() ++ chunk_110_content_1()),
  ok = stockdb_raw:close(S1),

  {ok, S2} = stockdb_raw:open(File, Options ++ [append]),
  ensure_states_equal(S1, S2),
  S3 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_raw:append(Event, State),
        NextState
    end, S2, chunk_110_content_2() ++ chunk_112_content()),
  ok = stockdb_raw:close(S3),

  {ok, S4_} = stockdb_raw:open(File, Options ++ [read]),
  {ok, S4} = stockdb_raw:restore_state(S4_),
  ensure_states_equal(S3, S4),
  ok = stockdb_raw:close(S4),

  {ok, FileEvents} = stockdb_raw:read_file(File),
  lists:zipwith(fun(Expected, Read) ->
        ensure_packets_equal(Expected, Read)
    end,
    chunk_109_content() ++ chunk_110_content_1() ++ chunk_110_content_2() ++ chunk_112_content(),
    FileEvents),
  ok = file:delete(File).

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
