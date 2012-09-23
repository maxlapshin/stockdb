-module(stockdb_reader_tests).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-import(stockdb_test_helper, [tempfile/1, tempdir/0, chunk_content/1]).

file_info_test() ->
  File = tempfile("write-append-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  stockdb_appender:write_events(File, chunk_content('109') ++ chunk_content('110_1'), 
    [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),

  ?assertEqual([{date,{2012,7,25}},{scale,200},{depth,3}, {presence,{289,[109,110]}}],
  	stockdb_reader:file_info(File, [date, scale, depth, presence])),
  ?assertEqual([{candle,{12.33,12.45,12.23,12.445}}], stockdb_reader:file_info(File, [candle])),
  file:delete(File).


candle_test() ->
  Root = stockdb:get_value(root),
  ok = application:set_env(fix, root, tempdir()),
  file:delete(stockdb_fs:path('TEST', "2012-07-25")),
  stockdb:write_events('TEST', "2012-07-25", chunk_content('109') ++ chunk_content('110_1'), [{scale, 200}]),
  Candle = stockdb:candle('TEST', "2012-07-25"),
  application:set_env(fix, root, Root),
  file:delete(stockdb_fs:path('TEST', "2012-07-25")),
  ?assertEqual({12.33,12.45,12.23,12.445}, Candle),
  ok.
