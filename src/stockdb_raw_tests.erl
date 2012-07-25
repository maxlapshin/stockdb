-module(stockdb_raw_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TESTDIR, "apps/stockdb/test").

-define(FIXTUREDIR, filename:join(?TESTDIR, "fixtures")).
-define(FIXTUREFILE(F), filename:join(?FIXTUREDIR, F)).

-define(TEMPDIR, filename:join(?TESTDIR, "temp")).
-define(TEMPFILE(F), filename:join(?TEMPDIR, F)).

file_create_test() ->
  check_creation_params([{stock, 'TEST'}, {date, {2012,7,26}}, {depth, 10}, {scale, 100}, {chunk_size, 300}],
    "TEST-20120726.300.10.100.stock"),
  check_creation_params([{stock, 'TEST'}, {date, {2012,7,26}}, {depth, 10}, {scale, 100}, {chunk_size, 300}],
    "TEST-20120726.300.10.100.stock").

check_creation_params(DBOptions, FixtureFile) ->
  true = filelib:is_dir(?TESTDIR),

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
