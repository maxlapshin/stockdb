%%% @doc Raw stockdb access library

-module(stockdb_raw).
-author({"Danil Zagoskin", z@gosk.in}).

-include("log.hrl").

-export([open/2, read/1, append/2, close/1]).

-define(STOCKDB_OPTIONS, [
    {vervion, 1},
    {stock, undefined},
    {date, utcdate()},
    {depth, 10},
    {scale, 100},
    {chunk_size, 300} % seconds
  ]).
-define(CHUNKUNITS, seconds). % This has to be function name in timer module

-record(dbstate, {
    file,
    stock,
    depth,
    scale,
    chunk_size,
    last_timestamp = 0,
    last_bidask,
    next_chunk_time = 0,
    chunk_map_offset
  }).

parse_opts([], State) ->
  State;
parse_opts([{stock, Stock}|MoreOpts], State) ->
  parse_opts(MoreOpts, State#dbstate{stock = Stock});
parse_opts([{depth, Depth}|MoreOpts], State) ->
  parse_opts(MoreOpts, State#dbstate{depth = Depth});
parse_opts([{scale, Scale}|MoreOpts], State) ->
  parse_opts(MoreOpts, State#dbstate{scale = Scale});
parse_opts([{chunk_size, CSize}|MoreOpts], State) ->
  parse_opts(MoreOpts, State#dbstate{chunk_size = CSize});
parse_opts([Unknown|MoreOpts], State) ->
  ?D({unknown_option, Unknown}),
  parse_opts(MoreOpts, State).


is_stockdb_option({Option, _}) ->
  lists:keymember(Option, 1, ?STOCKDB_OPTIONS);
is_stockdb_option(_) -> false.


utcdate() ->
  {Date, _Time} = calendar:universal_time(),
  Date.


open(FileName, Options) ->
  {StockDBOpts, OpenOpts} = lists:partition(fun is_stockdb_option/1, Options),

  {Action, FileOpts} = determine_action(OpenOpts),
  FileExists = filelib:is_regular(FileName),

  case {Action, FileExists} of
    {create, _} ->
      create_new_db(FileName, FileOpts, StockDBOpts);
    {read, _} ->
      open_existing_db(FileName, FileOpts, StockDBOpts);
    {append, true} ->
      open_existing_db(FileName, FileOpts, StockDBOpts);
    {append, false} ->
      create_new_db(FileName, FileOpts, StockDBOpts)
  end.

determine_action(FileOpts) ->
  determine_action(FileOpts, [], []).

determine_action([Mode|Opts], Modes, NonModes) when Mode == read orelse Mode == write orelse Mode == append ->
  determine_action(Opts, [Mode|Modes], NonModes);

determine_action([Opt|Opts], Modes, NonModes) ->
  determine_action(Opts, Modes, [Opt|NonModes]);

determine_action([], Modes, NonModes) ->
  case lists:usort(Modes) of
    [read] -> {read, [read|NonModes]};
    [write] -> {create, [write|NonModes]};
    [append] -> {append, [read, write|NonModes]};
    [read, write] -> {append, [read, write|NonModes]};
    Other -> erlang:error({unsupported_option_set, Other})
  end.

%% Here we create skeleton for new DB
create_new_db(FileName, FileOpts, GivenStockDBOpts) ->
  {ok, File} = file:open(FileName, [binary|FileOpts]),
  {ok, 0} = file:position(File, bof),
  ok = file:truncate(File),

  StockDBOpts = lists:ukeymerge(1,
    lists:ukeysort(1, GivenStockDBOpts),
    lists:ukeysort(1, ?STOCKDB_OPTIONS)),

  {ok, ChunkMapOffset} = write_header(File, StockDBOpts),
  {ok, _CMSize} = write_chunk_map(File, StockDBOpts),

  State0 = parse_opts(StockDBOpts, #dbstate{}),

  {ok, State0#dbstate{
      file = File,
      chunk_map_offset = ChunkMapOffset
    }}.

open_existing_db(FileName, FileOpts, GivenStockDBOpts) ->
  {ok, File} = file:open(FileName, [binary|FileOpts]),
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, ChunkMapOffset} = read_header(File),

  StockDBOpts = update_db_options(SavedDBOpts, GivenStockDBOpts),
  State0 = parse_opts(StockDBOpts, #dbstate{}),

  {ok, State0#dbstate{
      file = File,
      chunk_map_offset = ChunkMapOffset
    }}.


update_db_options(OldOptions, _NewOptions) ->
  % TODO: decide what we can update
  OldOptions.


read(FileName) ->
  {error, not_implemented}.

close(#dbstate{file = File} = _State) ->
  file:close(File).

append({md, Timestamp, Bid, Ask}, State) ->
  {error, not_implemented}.



write_header(File, StockDBOpts) ->
  ok = file:pwrite(File, bof, <<"#!/usr/bin/env stockdb\n">>),
  lists:foreach(fun({Key, Value}) ->
        ok = file:write(File, [io_lib:print(Key), ": ", io_lib:print(Value), "\n"])
    end, StockDBOpts),
  ok = file:write(File, "\n"),
  file:position(File, cur).

read_header(File) ->
  {error, not_implemented}.


write_chunk_map(File, StockDBOpts) ->
  ChunkSize = proplists:get_value(chunk_size, StockDBOpts),
  ChunkCount = erlang:round(timer:hours(24)/timer:?CHUNKUNITS(ChunkSize)) + 1,

  ChunkLen = 32,
  ChunkMap = [<<0:ChunkLen>> || _ <- lists:seq(1, ChunkCount)],
  Size = ChunkLen * ChunkCount,

  ok = file:write(File, ChunkMap),
  {ok, Size}.
