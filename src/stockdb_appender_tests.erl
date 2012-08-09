-module(stockdb_appender_tests).
-include("stockdb_test_content.hrl").


file_create_test() ->
  check_creation_params([{stock, 'TEST'}, {date, {2012,7,26}}, {depth, 10}, {scale, 100}, {chunk_size, 300}],
    "TEST-20120726.300.10.100.stock"),
  check_creation_params([{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 15}, {scale, 200}, {chunk_size, 600}],
    "TEST-20120725.600.15.200.stock").

check_creation_params(DBOptions, FixtureFile) ->
  ?assert(filelib:is_dir(?TESTDIR)),

  File = ?TEMPFILE("creation-test.temp"),
  file:delete(File),
  % ok = filelib:ensure_dir(File),
  % ok = file:write_file(File, "GARBAGE"),

  {ok, S} = stockdb_appender:open(File, DBOptions),
  ok = stockdb_appender:close(S),
  db_no_regress(?FIXTUREFILE(FixtureFile), File),
  ok = file:delete(File).

db_no_regress(OldFile, NewFile) ->
  % TODO: Make something intelligent
  ?assertEqual(file:read_file(OldFile), file:read_file(NewFile)).



write_append_test() ->
  File = ?TEMPFILE("write-append-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  {ok, S0} = stockdb_appender:open(File, [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_appender:append(Event, State),
        NextState
    end, S0, chunk_109_content() ++ chunk_110_content_1()),
  ok = stockdb_appender:close(S1),

  {ok, S2} = stockdb_appender:open(File, []),
  ensure_states_equal(S1, S2),
  S3 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_raw:append(Event, State),
        NextState
    end, S2, chunk_110_content_2() ++ chunk_112_content()),
  ok = stockdb_raw:close(S3),

  % {ok, S4_} = stockdb_raw:open(File, Options ++ [read]),
  % {ok, S4} = stockdb_raw:restore_state(S4_),
  % ensure_states_equal(S3, S4),
  % ok = stockdb_raw:close(S4),

  {ok, FileEvents} = stockdb_raw:read_file(File),
  lists:zipwith(fun(Expected, Read) ->
        ensure_packets_equal(Expected, Read)
    end,
    chunk_109_content() ++ chunk_110_content_1() ++ chunk_110_content_2() ++ chunk_112_content(),
    FileEvents),
  ok = file:delete(File).
