-module(stockdb_validator_tests).

-include_lib("eunit/include/eunit.hrl").

-import(stockdb_test_helper, [tempfile/1, fixturefile/1, ensure_states_equal/2, write_events_to_file/2, append_events_to_file/2, ensure_packets_equal/2, chunk_content/1]).

db_repair_test() ->
  File = tempfile("db-repair-test.temp"),
  write_events_to_file(File, chunk_content('109') ++ chunk_content('110_1')),

  {ok, F} = file:open(File, [read, write]),
  {ok, _} = file:position(F, {eof, -1}),
  ok = file:truncate(F),
  ok = file:close(F),
  
  append_events_to_file(File, chunk_content('110_2') ++ chunk_content('112')),

  {ok, FileEvents} = stockdb_reader:read_file(File),
  lists:zipwith(fun(Expected, Read) ->
        ensure_packets_equal(Expected, Read)
    end,
    chunk_content('109') ++ chunk_content('110_1_t') ++ chunk_content('110_2') ++ chunk_content('112'),
    FileEvents),

  ok = file:delete(File).
