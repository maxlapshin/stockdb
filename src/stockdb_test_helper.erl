-module(stockdb_test_helper).
%%% Testing stuff.
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("stockdb.hrl").


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
  file:delete(File),
  append_events_to_file(File, Events).

%% Simply append events to file
append_events_to_file(File, Events) ->
  {ok, S0} = stockdb_appender:open(File, [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = stockdb_appender:append(Event, State),
        NextState
    end, S0, Events),
  ok = stockdb:close(S1).

%% Comparing stuff.
ensure_states_equal(State1, State2) ->
  Names = record_info(fields, dbstate),
  Blacklist = [file, buffer, buffer_end],
  F = fun(State) -> [{K,V} || {K,V} <- lists:zip(Names, tl(tuple_to_list(State))), not lists:member(K,Blacklist)] end,
  lists:zipwith(fun
      ({last_md, MD1}, {last_md, MD2}) ->
        ensure_packets_equal(MD1, MD2);
      (El1, El2) ->
        ?assertEqual(El1, El2)
  end, F(State1), F(State2)).

ensure_packets_equal(P, P) -> ok;
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
ensure_bidask_equal([Extra|BA1], []) ->
  ?assertEqual({0.0, 0}, Extra),
  ensure_bidask_equal(BA1, []);
ensure_bidask_equal([], []) ->
  true.

compare_eventlists([], []) ->
  ok;
compare_eventlists(EL1, []) ->
  {first_tail, length(EL1)};
compare_eventlists([], EL2) ->
  {second_tail, length(EL2)};
compare_eventlists([P1|EL1], [P2|EL2]) ->
  case packets_equal(P1, P2) of
    true ->
      compare_eventlists(EL1, EL2);
    false ->
      handle_nonequal([P1|EL1], [P2|EL2])
  end.

handle_nonequal([P1|EL1], [P2|EL2]) ->
  TS1 = element(2, P1),
  TS2 = element(2, P2),
  {Message, NL1, NL2} = if
    TS1 == TS2 ->
      {{error, P1, P2}, EL1, EL2};
    TS1 < TS2 ->
      {GapLen, AL1} = feed_until(TS2, EL1, 0),
      {{second_gap, TS1, TS2, GapLen}, AL1, EL2};
    TS1 > TS2 ->
      {GapLen, AL2} = feed_until(TS1, EL2, 0),
      {{first_gap, TS1, TS2, GapLen}, EL1, AL2}
  end,
  [Message|compare_eventlists(NL1, NL2)].

packets_equal(P, P) -> true;
packets_equal({trade, TS1, P1, V} = P1, {trade, TS2, P2, V} = P2) ->
  abs(TS1 - TS2) < 10 andalso abs(P1 - P2) < 0.0001;
packets_equal({md, TS1, Bid1, Ask1}, {md, TS2, Bid2, Ask2}) ->
  abs(TS1 - TS2) < 10 andalso bidask_equal(Bid1, Bid2) andalso bidask_equal(Ask1, Ask2);
packets_equal(_, _) -> false.

bidask_equal([], []) -> true;
bidask_equal([{P1, V}|BA1], [{P2, V}|BA2]) ->
  abs(P1 - P2) < 0.0001 andalso bidask_equal(BA1, BA2);
bidask_equal(_, _) -> false.


feed_until(_TS, [], Count) ->
  {Count, []};
feed_until(TS, [E|EL], Count) ->
  case element(2, E) of
    Small when Small < TS ->
      feed_until(TS, EL, Count + 1);
    _ ->
      {Count, [E|EL]}
  end.
