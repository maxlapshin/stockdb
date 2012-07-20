%%% @doc Raw stockdb access library

-module(stockdb_raw).
-author({"Danil Zagoskin", z@gosk.in}).

-include("log.hrl").

-export([open/2, read/1, append/2, close/1]).

-define(STOCKDB_OPTIONS, [
    {version, 1},
    {stock, undefined},
    {date, utcdate()},
    {depth, 10},
    {scale, 100},
    {chunk_size, 300} % seconds
  ]).
-define(CHUNKUNITS, seconds). % This has to be function name in timer module
-define(OFFSETLEN, 32).

-record(dbstate, {
    version,
    file,
    stock,
    date,
    depth,
    scale,
    chunk_size,
    last_timestamp = 0,
    last_bidask,
    next_chunk_time = 0,
    chunk_map_offset,
    daystart
  }).

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


append({md, Timestamp, _Bid, _Ask} = MD, #dbstate{last_bidask = undefined} = State) ->
  append_full_md(MD, start_chunk(Timestamp, State));

append({md, Timestamp, _Bid, _Ask} = MD, #dbstate{next_chunk_time = NCT} = State) when Timestamp >= NCT ->
  append_full_md(MD, start_chunk(Timestamp, State));

append({md, _Timestamp, _Bid, _Ask} = MD, #dbstate{} = State) ->
  append_delta_md(MD, State).


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

  ChunkMap = [<<0:?OFFSETLEN>> || _ <- lists:seq(1, ChunkCount)],
  Size = ?OFFSETLEN * ChunkCount,

  ok = file:write(File, ChunkMap),
  {ok, Size}.

start_chunk(Timestamp, #dbstate{daystart = undefined, date = Date} = State) ->
  DaystartSeconds = calendar:datetime_to_gregorian_seconds({Date, {0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  Daystart = DaystartSeconds * 1000,
  start_chunk(Timestamp, State#dbstate{daystart = Daystart});

start_chunk(Timestamp, State) ->
  #dbstate{
    daystart = Daystart,
    chunk_size = ChunkSize,
    chunk_map_offset = ChunkMapOffset,
    file = File} = State,

  ChunkSizeMs = timer:?CHUNKUNITS(ChunkSize),
  ChunkNumber = (Timestamp - Daystart) div ChunkSizeMs,

  {ok, EOF} = file:position(File, eof),
  ChunkOffset = EOF - ChunkMapOffset,

  ok = file:pwrite(File, {bof, ChunkMapOffset + ChunkNumber*?OFFSETLEN}, <<ChunkOffset:?OFFSETLEN>>),

  NextChunkTime = Daystart + ChunkSizeMs * (ChunkNumber + 1),

  State#dbstate{next_chunk_time = NextChunkTime}.
  

append_full_md({md, Timestamp, Bid, Ask}, #dbstate{depth = Depth, file = File} = State) ->
  BidAsk = [setdepth(Bid, Depth), setdepth(Ask, Depth)],
  Data = stockdb_format:encode_full_md(Timestamp, BidAsk),
  ok = file:pwrite(File, eof, Data),
  {ok, State#dbstate{
      last_timestamp = Timestamp,
      last_bidask = BidAsk}
  }.

append_delta_md({md, Timestamp, Bid, Ask}, #dbstate{depth = Depth, file = File, last_timestamp = LastTS, last_bidask = LastBA} = State) ->
  BidAsk = [setdepth(Bid, Depth), setdepth(Ask, Depth)],
  BidAskDelta = bidask_delta(LastBA, BidAsk),
  Data = stockdb_format:encode_delta_md(Timestamp - LastTS, BidAskDelta),
  ok = file:pwrite(File, eof, Data),
  {ok, State#dbstate{
      last_timestamp = Timestamp,
      last_bidask = BidAsk}
  }.

setdepth(_Quotes, 0) ->
  [];
setdepth([], Depth) ->
  [{0, 0} || _ <- lists:seq(1, Depth)];
setdepth([Q|Quotes], Depth) ->
  [Q|setdepth(Quotes, Depth - 1)].

bidask_delta([[_|_] = Bid1, [_|_] = Ask1], [[_|_] = Bid2, [_|_] = Ask2]) ->
  [bidask_delta1(Bid1, Bid2), bidask_delta1(Ask1, Ask2)].

bidask_delta1(List1, List2) ->
  lists:zipwith(fun({Price1, Volume1}, {Price2, Volume2}) ->
    {Price2 - Price1, Volume2 - Volume1}
  end, List1, List2).

