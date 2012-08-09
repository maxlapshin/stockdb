%%% @doc Raw stockdb access library

-module(stockdb_raw).
-author({"Danil Zagoskin", z@gosk.in}).

-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("stockdb.hrl").

-export([init_with_opts/1]).
-export([number_of_chunks/1]).

-define(PARSEOPT(OptName),
  parse_opts([{OptName, Value}|MoreOpts], State) ->
    parse_opts(MoreOpts, State#dbstate{OptName = Value})).

parse_opts([], State) -> State;
?PARSEOPT(version);
?PARSEOPT(file);
?PARSEOPT(stock);
?PARSEOPT(date);
?PARSEOPT(depth);
?PARSEOPT(scale);
?PARSEOPT(chunk_size);
?PARSEOPT(buffer);
parse_opts([Unknown|MoreOpts], State) ->
  ?D({unknown_option, Unknown}),
  parse_opts(MoreOpts, State).


is_stockdb_option({Option, _}) ->
  lists:keymember(Option, 1, ?STOCKDB_OPTIONS);
is_stockdb_option(_) -> false.


utcdate() ->
  {Date, _Time} = calendar:universal_time(),
  Date.

init_with_opts(Opts) ->
  parse_opts(Opts, #dbstate{}).



update_db_options(OldOptions, _NewOptions) ->
  % TODO: decide what we can update
  OldOptions.



%read_file(FileName) ->
%  {ok, State0} = stockdb_reader:open(FileName),
%  {ok, FileSize} = file:position(State0#dbstate.file, eof),
%  [{_, ROffset0}|_] = stockdb_raw:nonzero_chunks(State0),
%
%  Offset0 = State0#dbstate.chunk_map_offset + ROffset0,
%  {ok, Buffer} = file:pread(State0#dbstate.file, Offset0, FileSize - Offset0),
%  close(State0),
%
%  {Events, _State1} = read_buffered_events(State0#dbstate{buffer = Buffer}),
%  {ok, Events}.


number_of_chunks(ChunkSize) ->
  timer:hours(24) div timer:seconds(ChunkSize) + 1.






utc_to_daystart(UTC) ->
  DayLength = timer:hours(24),
  DayTail = UTC rem DayLength,
  UTC - DayTail.


 

