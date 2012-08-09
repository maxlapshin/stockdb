-module(stockdb_test_helper).
%%% Testing stuff.
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").



fixturedir() ->
  filename:join(testdir(), "fixtures").


fixturefile(F) ->
  filename:join(fixturedir(), F).


testdir() ->
  code:lib_dir(stockdb, test).

tempdir() ->
  filename:join(testdir(), "temp").

tempfile(F) ->
  filename:join(tempdir(), F).


chunk_content('109') -> chunk_109_content();
chunk_content('110_1_t') -> chunk_110_content_1_trunc();
chunk_content('110_1') -> chunk_110_content_1();
chunk_content('110_2') -> chunk_110_content_2();
chunk_content('112') -> chunk_112_content();
chunk_content('full') -> full_content().

%% Test content
chunk_109_content() ->
  [
    {md, 1343207118230, [{12.34, 715}, {12.195, 201}, {11.97, 1200}], [{12.435, 601}, {12.47, 1000}, {12.60, 800}]},
    {md, 1343207154170, [{12.34, 500}, {12.185, 201}, {11.97, 1200}], [{12.440, 601}, {12.47, 1000}, {12.60, 850}]},
    {md, 1343207197200, [{12.34, 715}, {12.195, 201}, {11.97, 1500}], [{12.435, 601}, {12.49, 1000}, {12.65, 850}]},
    {md, 1343207251182, [{12.34, 715}, {12.195, 201}, {11.97, 1200}], [{12.435, 700}, {12.47, 1000}, {12.60, 600}]},
 {trade, 1343207273291, 12.33, 490},
    {md, 1343207291284, [{12.34, 300}, {12.195, 120}, {11.97, 1200}], [{12.435, 800}, {12.47, 1000}, {12.65, 600}]},
    {md, 1343207307670, [{12.32, 800}, {12.170, 400}, {11.97, 1100}], [{12.440, 800}, {12.47, 1100}, {12.69, 600}]},
    {md, 1343207326562, [{12.34, 300}, {12.195, 120}, {11.97, 1200}], [{12.435, 800}, {12.47, 1000}, {12.65, 600}]},
 {trade, 1343207362719, 12.44, 200},
    {md, 1343207382471, [{12.34, 300}, {12.195, 120}, {11.97, 1200}], [{12.435, 650}, {12.47,  950}, {12.65, 600}]}
  ].

chunk_110_content_1_trunc() ->
  [
 {trade, 1343207402486, 12.445, 300},
    {md, 1343207410324, [{12.35, 800}, {12.270, 450}, {11.97, 1200}], [{12.435, 450}, {12.47,  850}, {12.65, 600}]}
  ].

chunk_110_content_1() ->
  chunk_110_content_1_trunc() ++ [
    {md, 1343207417957, [{12.35, 800}, {12.270, 450}, {11.97, 1200}], [{12.450, 800}, {12.49, 1000}, {12.65, 600}]}
  ].

chunk_110_content_2() ->
  [
    {md, 1343207600274, [{12.35, 700}, {12.265, 400}, {11.97, 1100}], [{12.440, 450}, {12.48, 1200}, {12.65, 800}]},
    {md, 1343207633713, [{12.34, 800}, {12.270, 300}, {11.97, 1200}], [{12.450, 800}, {12.49, 1000}, {12.65, 600}]},
 {trade, 1343207652486, 12.45, 200}
  ].

chunk_112_content() ->
  [
    {md, 1343208100274, [{12.35, 700}, {12.265, 400}, {11.97, 1100}], [{12.440, 450}, {12.48, 1200}, {12.65, 800}]}
  ].

full_content() ->
  chunk_109_content() ++ chunk_110_content_1() ++ chunk_110_content_2() ++ chunk_112_content().


%% Simply write events to file
write_events_to_file(File, Events) ->
  ok = filelib:ensure_dir(File),
  {ok, S0} = stockdb_raw:open(File, [write, {stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_raw:append(Event, State),
        NextState
    end, S0, Events),
  ok = stockdb_raw:close(S1).

%% Simply append events to file
append_events_to_file(File, Events) ->
  {ok, S0} = stockdb_raw:open(File, [append, {stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_raw:append(Event, State),
        NextState
    end, S0, Events),
  ok = stockdb_raw:close(S1).

%% Comparing stuff.
ensure_states_equal(State1, State2) ->
  Elements = lists:seq(1, size(State1)) -- [3, 4, 5],
  lists:foreach(fun(N) ->
        % io:format("Comparing element ~w~n", [N]),
        ?assertEqual(element(N, State1), element(N, State2))
    end, Elements).

ensure_packets_equal({trade, _, _, _} = P1, {trade, _, _, _} = P2) ->
  ?assertEqual(P1, P2);
ensure_packets_equal({md, TS1, Bid1, Ask1}, {md, TS2, Bid2, Ask2}) ->
  ?assertEqual(TS1, TS2),
  ensure_bidask_equal(Bid1, Bid2),
  ensure_bidask_equal(Ask1, Ask2).

ensure_bidask_equal([{P1, V1}|BA1], [{P2, V2}|BA2]) ->
  ?assertEqual(V1, V2),
  ?assert(abs(P1 - P2) < 0.00001),
  ensure_bidask_equal(BA1, BA2);
ensure_bidask_equal([], [Extra|BA2]) ->
  ?assertEqual({0.0, 0}, Extra),
  ensure_bidask_equal([], BA2);
ensure_bidask_equal([], []) ->
  true.
