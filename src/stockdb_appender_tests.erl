-module(stockdb_appender_tests).
-include_lib("eunit/include/eunit.hrl").

-import(stockdb_test_helper, [tempfile/1, fixturefile/1, ensure_states_equal/2, write_events_to_file/2, append_events_to_file/2, ensure_packets_equal/2, chunk_content/1]).


file_create_test() ->
  check_creation_params([{stock, 'TEST'}, {date, {2012,7,26}}, {depth, 10}, {scale, 100}, {chunk_size, 300}],
    "TEST-20120726.300.10.100.stock"),
  check_creation_params([{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 15}, {scale, 200}, {chunk_size, 600}],
    "TEST-20120725.600.15.200.stock").

check_creation_params(DBOptions, FixtureFile) ->
  File = tempfile("creation-test.temp"),
  file:delete(File),
  % ok = filelib:ensure_dir(File),
  % ok = file:write_file(File, "GARBAGE"),

  {ok, S} = stockdb_appender:open(File, DBOptions),
  ok = stockdb_appender:close(S),
  db_no_regress(fixturefile(FixtureFile), File),
  ok = file:delete(File).

db_no_regress(OldFile, NewFile) ->
  % TODO: Make something intelligent
  ?assertEqual(file:read_file(OldFile), file:read_file(NewFile)).



write_append_test() ->
  File = tempfile("write-append-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  {ok, S0} = stockdb_appender:open(File, [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_appender:append(Event, State),
        NextState
    end, S0, chunk_content('109') ++ chunk_content('110_1')),
  ok = stockdb_appender:close(S1),

  {ok, S2} = stockdb_appender:open(File, []),
  ensure_states_equal(S1, S2),
  S3 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_appender:append(Event, State),
        NextState
    end, S2, chunk_content('110_2') ++ chunk_content('112')),
  ok = stockdb_raw:close(S3),

  % {ok, S4_} = stockdb_raw:open(File, Options ++ [read]),
  % {ok, S4} = stockdb_raw:restore_state(S4_),
  % ensure_states_equal(S3, S4),
  % ok = stockdb_raw:close(S4),

  {ok, FileEvents} = stockdb_raw:read_file(File),
  lists:zipwith(fun(Expected, Read) ->
        ensure_packets_equal(Expected, Read)
    end,
    chunk_content('109') ++ chunk_content('110_1') ++ chunk_content('110_2') ++ chunk_content('112'),
    FileEvents),
  ok = file:delete(File).


db_repair_test() ->
  File = tempfile("db-repair-test.temp"),
  write_events_to_file(File, chunk_content('109') ++ chunk_content('110_1')),

  {ok, F} = file:open(File, [read, write]),
  {ok, _} = file:position(F, {eof, -1}),
  ok = file:truncate(F),
  ok = file:close(F),

  {ok, S1} = stockdb_raw:open(File, [read]),
  ?assertThrow({truncate_failed, _}, stockdb_raw:restore_state(S1)),

  append_events_to_file(File, chunk_content('110_2') ++ chunk_content('112')),

  {ok, FileEvents} = stockdb_raw:read_file(File),
  lists:zipwith(fun(Expected, Read) ->
        ensure_packets_equal(Expected, Read)
    end,
    chunk_content('109') ++ chunk_content('110_1_t') ++ chunk_content('110_2') ++ chunk_content('112'),
    FileEvents),

  ok = file:delete(File).


