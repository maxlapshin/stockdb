-module(stockdb_reader_tests).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-import(stockdb_test_helper, [tempfile/1, chunk_content/1]).

file_info_test() ->
  File = tempfile("write-append-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  {ok, S0} = stockdb_appender:open(File, [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_appender:append(Event, State),
        NextState
    end, S0, chunk_content('109') ++ chunk_content('110_1')),
  ok = stockdb_appender:close(S1),

  ?assertEqual([{date,{2012,7,25}},{scale,200},{depth,3}, {presence,{289,[109,110]}}],
  	stockdb_reader:file_info(File, [date, scale, depth, presence])),
  ?assertEqual([{candle,{12.33,12.45,12.23,12.445}}], stockdb_reader:file_info(File, [candle])),
  file:delete(File).


