-module(stockdb_appender_tests).
-include("stockdb_test_content.hrl").


file_create_test() ->
  file_create_test([]).

file_create_test(Modes) ->
  check_creation_params(Modes ++ [{stock, 'TEST'}, {date, {2012,7,26}}, {depth, 10}, {scale, 100}, {chunk_size, 300}],
    "TEST-20120726.300.10.100.stock"),
  check_creation_params(Modes ++ [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 15}, {scale, 200}, {chunk_size, 600}],
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


