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


init_with_opts(Opts) ->
  parse_opts(Opts, #dbstate{}).


number_of_chunks(ChunkSize) ->
  timer:hours(24) div timer:seconds(ChunkSize) + 1.
