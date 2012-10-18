-module(stockdb_appender_tests).
-include_lib("eunit/include/eunit.hrl").

-import(stockdb_test_helper, [tempfile/1, tempdir/0, fixturefile/1, ensure_states_equal/2, write_events_to_file/2, append_events_to_file/2, ensure_packets_equal/2, chunk_content/1]).


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


append_typed_stock_test() ->
  application:load(stockdb),
  application:set_env(stockdb, root, tempdir()),
  Path = tempdir() ++ "/daily/TEST-2012-07-25.stock",
  file:delete(Path),
  {ok, DB} = stockdb:open_append({daily, 'TEST'}, "2012-07-25", [{depth,3}]),
  Info = stockdb:info(DB),
  ?assertEqual(Path, proplists:get_value(path, Info)),
  file:delete(Path),
  ok.

append_bm_test() ->
  File = tempfile("append-bm-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  Count = 80000,
  Date = {2012,7,25},
  StartTS = (calendar:datetime_to_gregorian_seconds({Date,{6,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}))*1000,
  {ok, S0} = stockdb_appender:open(File, [nosync, {stock, 'TEST'},{date, Date}, {depth, 1}, {scale, 100}]),
  T1 = erlang:now(),
  S1 = fill_records(S0, Count, 10, StartTS),
  T2 = erlang:now(),
  stockdb_appender:close(S1),
  file:delete(File),
  Delta = timer:now_diff(T2,T1),
  ?debugFmt("Append benchmark: ~B in ~B ms, about ~B us per row", [Count, Delta div 1000, Delta div Count]),
  ok.

fill_records(S0, Count, _, _) when Count =< 0 ->
  S0;

fill_records(S0, Count, Step, TS) ->
  {ok, S1} = stockdb:append({md, TS+0*Step, [{43.15, 50}], [{45.15, 50}]}, S0),
  {ok, S2} = stockdb:append({md, TS+1*Step, [{43.09, 40}], [{45.05, 50}]}, S1),
  {ok, S3} = stockdb:append({md, TS+2*Step, [{42.50, 20}], [{44.95, 20}]}, S2),
  {ok, S4} = stockdb:append({md, TS+3*Step, [{42.05, 90}], [{43.15, 50}]}, S3),
  {ok, S5} = stockdb:append({md, TS+4*Step, [{42.45, 50}], [{44.15, 40}]}, S4),
  {ok, S6} = stockdb:append({md, TS+5*Step, [{41.55, 54}], [{44.15, 50}]}, S5),
  {ok, S7} = stockdb:append({md, TS+6*Step, [{42.12, 10}], [{45.15, 30}]}, S6),
  {ok, S8} = stockdb:append({md, TS+7*Step, [{42.80, 90}], [{43.15, 80}]}, S7),
  {ok, S9} = stockdb:append({md, TS+8*Step, [{43.12, 20}], [{44.15, 50}]}, S8),
  {ok, S_} = stockdb:append({md, TS+9*Step, [{43.45, 20}], [{45.15, 50}]}, S9),
  fill_records(S_, Count - 10, Step, TS + 10*Step).



write_append_test() ->
  File = tempfile("write-append-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  {ok, S0} = stockdb_appender:open(File, [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_appender:append(Event, State),
        NextState
    end, S0, chunk_content('109') ++ [hd(chunk_content('110_1'))]),
  ok = stockdb_appender:close(S1),

  {ok, S1_1} = stockdb_appender:open(File, []),
  ensure_states_equal(S1, S1_1),

  S1_2 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_appender:append(Event, State),
        NextState
    end, S1_1, tl(chunk_content('110_1'))),
  ok = stockdb_appender:close(S1_2),

  {ok, S2} = stockdb_appender:open(File, []),
  ensure_states_equal(S1_2, S2),
  
  S3 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_appender:append(Event, State),
        NextState
    end, S2, chunk_content('110_2') ++ chunk_content('112')),
  ok = stockdb_appender:close(S3),
  
  % {ok, S4_} = stockdb_raw:open(File, Options ++ [read]),
  % {ok, S4} = stockdb_raw:restore_state(S4_),
  % ensure_states_equal(S3, S4),
  % ok = stockdb_raw:close(S4),

  FileEvents = stockdb:events({path, File}, undefined),

  lists:zipwith(fun(Expected, Read) ->
        ensure_packets_equal(Expected, Read)
    end,
    chunk_content('109') ++ chunk_content('110_1') ++ chunk_content('110_2') ++ chunk_content('112'),
    FileEvents),
  ok = file:delete(File).


append_verifier_test() ->
  File = tempfile("append-verifier-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  {ok, S0} = stockdb_appender:open(File, [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),

  ?assertThrow({_, bad_timestamp, _}, stockdb_appender:append({trade, undefined, 2.34, 29}, S0)),
  ?assertThrow({_, bad_price, _}, stockdb_appender:append({trade, 1350575093098, undefined, 29}, S0)),
  ?assertThrow({_, bad_volume, _}, stockdb_appender:append({trade, 1350575093098, 2.34, -29}, S0)),

  ?assertThrow({_, bad_timestamp, _}, stockdb_appender:append({md, undefined, [{2.34, 29}], [{2.35, 31}]}, S0)),
  ?assertThrow({_, bad_bid, _}, stockdb_appender:append({md, 1350575093098, [], [{2.35, 31}]}, S0)),
  ?assertThrow({_, bad_bid, _}, stockdb_appender:append({md, 1350575093098, undefined, [{2.35, 31}]}, S0)),
  ?assertThrow({_, bad_ask, _}, stockdb_appender:append({md, 1350575093098, [{2.34, 29}], [{2.35, -31}]}, S0)),
  ?assertThrow({_, bad_ask, _}, stockdb_appender:append({md, 1350575093098, [{2.34, 29}], [{[], 31}]}, S0)),

  ?assertThrow({_, invalid_event, _}, stockdb_appender:append({other, 1350575093098, [{2.34, 29}], [{12, 31}]}, S0)),

  ok = stockdb_appender:close(S0),
  ok = file:delete(File).
