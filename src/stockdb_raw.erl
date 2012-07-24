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
    buffer,
    stock,
    date,
    depth,
    scale,
    chunk_size,
    last_timestamp = 0,
    last_bidask,
    next_chunk_time = 0,
    chunk_map_offset,
    chunk_map = [],
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

  StateFileOpen = State0#dbstate{
    file = File,
    chunk_map_offset = ChunkMapOffset},

  StateChunkRead = read_chunk_map(StateFileOpen),

  StateReady = fast_forward(StateChunkRead),

  ?D({last_packet, StateReady#dbstate.last_timestamp}),
  {ok, StateReady}.


update_db_options(OldOptions, _NewOptions) ->
  % TODO: decide what we can update
  OldOptions.


read(FileName) ->
  {ok, State0} = open(FileName, [read, binary]),
  {ok, FileSize} = file:position(State0#dbstate.file, eof),
  [{_, ROffset0}|_] = nonzero_chunks(State0),

  Offset0 = State0#dbstate.chunk_map_offset + ROffset0,
  {ok, Buffer} = file:pread(State0#dbstate.file, {bof, Offset0}, FileSize - Offset0),
  close(State0),

  {Events, _State1} = read_buffered_events(State0#dbstate{buffer = Buffer}),
  {ok, Events}.

close(#dbstate{file = File} = _State) ->
  file:close(File).


append({md, Timestamp, Bid, Ask}, #dbstate{scale = Scale} = State) ->
  SBid = apply_scale(Bid, Scale),
  SAsk = apply_scale(Ask, Scale),
  append({scaled, {md, Timestamp, SBid, SAsk}}, State);

append({scaled, {md, Timestamp, _Bid, _Ask} = MD}, #dbstate{last_bidask = undefined} = State) ->
  append_full_md(MD, start_chunk(Timestamp, State));

append({scaled, {md, Timestamp, _Bid, _Ask} = MD}, #dbstate{next_chunk_time = NCT} = State) when Timestamp >= NCT ->
  append_full_md(MD, start_chunk(Timestamp, State));

append({scaled, {md, _Timestamp, _Bid, _Ask} = MD}, #dbstate{} = State) ->
  append_delta_md(MD, State).


write_header(File, StockDBOpts) ->
  ok = file:pwrite(File, bof, <<"#!/usr/bin/env stockdb\n">>),
  lists:foreach(fun({Key, Value}) ->
        ok = file:write(File, [io_lib:print(Key), ": ", stockdb_format:format_header_value(Key, Value), "\n"])
    end, StockDBOpts),
  ok = file:write(File, "\n"),
  file:position(File, cur).

read_header(File) ->
  Options = read_header_lines(File, []),
  {ok, Offset} = file:position(File, cur),
  {ok, Options, Offset}.

read_header_lines(File, Acc) ->
  {ok, HeaderLine} = file:read_line(File),
  case parse_header_line(HeaderLine) of
    {Key, Value} ->
      read_header_lines(File, [{Key, Value}|Acc]);
    ignore ->
      read_header_lines(File, Acc);
    stop ->
      lists:reverse(Acc)
  end.

parse_header_line(HeaderLine) when is_binary(HeaderLine) ->
  parse_header_line(erlang:binary_to_list(HeaderLine));

parse_header_line("#" ++ _Comment) ->
  ignore;

parse_header_line("\n") ->
  stop;

parse_header_line(HeaderLine) when is_list(HeaderLine) ->
  parse_header_line(string:strip(HeaderLine, right, $\n), nonewline).

parse_header_line(HeaderLine, nonewline) ->
  [KeyRaw, ValueRaw] = string:tokens(HeaderLine, ":"),

  KeyStr = string:strip(KeyRaw, both),
  ValueStr = string:strip(ValueRaw, both),

  Key = erlang:list_to_atom(KeyStr),
  Value = stockdb_format:parse_header_value(Key, ValueStr),

  {Key, Value}.

% FIXME: number_of_chunks(300) = 289   -— WTF? Should be 288
number_of_chunks(ChunkSize) ->
  timer:hours(24) div timer:?CHUNKUNITS(ChunkSize) + 1.

write_chunk_map(File, StockDBOpts) ->
  ChunkSize = proplists:get_value(chunk_size, StockDBOpts),
  ChunkCount = number_of_chunks(ChunkSize),

  ChunkMap = [<<0:?OFFSETLEN>> || _ <- lists:seq(1, ChunkCount)],
  Size = ?OFFSETLEN * ChunkCount,

  ok = file:write(File, ChunkMap),
  {ok, Size}.

read_chunk_map(#dbstate{} = State) ->
  ChunkMap = lists:map(fun({Number, Offset}) ->
        {md, Timestamp, _Bid, _Ask} = read_full_md_at_offset(Offset, State),
        {Number, Timestamp, Offset}
    end, nonzero_chunks(State)),
  State#dbstate{chunk_map = ChunkMap}.


nonzero_chunks(#dbstate{file = File, chunk_size = ChunkSize, chunk_map_offset = ChunkMapOffset}) ->
  OffsetByteSize = ?OFFSETLEN div 8,
  ChunkCount = number_of_chunks(ChunkSize),
  ReversedResult = lists:foldl(fun(N, NZChunks) ->
        case file:pread(File, {bof, ChunkMapOffset + OffsetByteSize*N}, OffsetByteSize) of
          {ok, <<0:?OFFSETLEN>>} ->
            NZChunks;
          {ok, <<NonZero:?OFFSETLEN/integer>>} ->
            [{N, NonZero}|NZChunks]
        end
    end, [], lists:seq(0, ChunkCount - 1)),
  lists:reverse(ReversedResult).


daystart(Date) ->
  DaystartSeconds = calendar:datetime_to_gregorian_seconds({Date, {0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  DaystartSeconds * 1000.

utc_to_daystart(UTC) ->
  DayLength = timer:hours(24),
  DayTail = UTC rem DayLength,
  UTC - DayTail.


start_chunk(Timestamp, #dbstate{daystart = undefined, date = Date} = State) ->
  start_chunk(Timestamp, State#dbstate{daystart = daystart(Date)});

start_chunk(Timestamp, State) ->
  #dbstate{
    daystart = Daystart,
    chunk_size = ChunkSize,
    chunk_map_offset = ChunkMapOffset,
    chunk_map = ChunkMap,
    file = File} = State,

  ChunkSizeMs = timer:?CHUNKUNITS(ChunkSize),
  ChunkNumber = (Timestamp - Daystart) div ChunkSizeMs,

  % sanity check
  (Timestamp - Daystart) < timer:hours(24) orelse erlang:error({not_this_day, Timestamp}),

  {ok, EOF} = file:position(File, eof),
  ChunkOffset = EOF - ChunkMapOffset,

  ByteOffsetLen = ?OFFSETLEN div 8,
  ok = file:pwrite(File, {bof, ChunkMapOffset + ChunkNumber*ByteOffsetLen}, <<ChunkOffset:?OFFSETLEN/integer>>),

  NextChunkTime = Daystart + ChunkSizeMs * (ChunkNumber + 1),

  Chunk = {ChunkNumber, Timestamp, ChunkOffset},
  ?D({new_chunk, Chunk}),
  State#dbstate{
    chunk_map = ChunkMap ++ [Chunk],
    next_chunk_time = NextChunkTime}.
  

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

bidask_delta_apply([[_|_] = Bid1, [_|_] = Ask1], [[_|_] = Bid2, [_|_] = Ask2]) ->
  [bidask_delta_apply1(Bid1, Bid2), bidask_delta_apply1(Ask1, Ask2)].

bidask_delta_apply1(List1, List2) ->
  lists:zipwith(fun({Price, Volume}, {DPrice, DVolume}) ->
    {Price + DPrice, Volume + DVolume}
  end, List1, List2).


fast_forward(#dbstate{chunk_map = []} = State) ->
  State;
fast_forward(#dbstate{file = File, chunk_size = ChunkSize, chunk_map_offset = ChunkMapOffset, chunk_map = ChunkMap} = State) ->
  {N, LastChunkTimestamp, LastChunkOffset} = lists:last(ChunkMap),
  AbsOffset = ChunkMapOffset + LastChunkOffset,
  {ok, FileSize} = file:position(File, eof),

  {ok, Buffer} = file:pread(File, {bof, AbsOffset}, FileSize - AbsOffset),
  {_Packets, LastState} = read_buffered_events(State#dbstate{buffer = Buffer}),

  Daystart = utc_to_daystart(LastChunkTimestamp),
  ChunkSizeMs = timer:?CHUNKUNITS(ChunkSize),

  LastState#dbstate{
    daystart = Daystart,
    next_chunk_time = Daystart + ChunkSizeMs * (N + 1)}.


read_full_md_at_offset(Offset, #dbstate{file = File, chunk_map_offset = ChunkMapOffset, depth = Depth} = State) ->
  PacketLen = 8 + 2 * 2 * 4 * Depth, % Timestamp 64 bit + (bid, ask) * (price, volume) * 32 bit * depth
  {ok, Buffer} = file:pread(File, {bof, ChunkMapOffset + Offset}, PacketLen),

  {Timestamp, BidAsk, _Tail} = stockdb_format:decode_full_md(Buffer, State#dbstate.depth),
  packet_from_mdentry(Timestamp, BidAsk, State).


read_buffered_events(State) ->
  read_buffered_events(State, []).

read_buffered_events(#dbstate{buffer = <<>>} = State, RevEvents) ->
  {lists:reverse(RevEvents), State};
read_buffered_events(#dbstate{} = State, RevEvents) ->
  {Event, NewState} = read_packet_from_buffer(State),
  read_buffered_events(NewState, [Event|RevEvents]).

read_packet_from_buffer(#dbstate{buffer = Buffer} = State) ->
  case stockdb_format:packet_type(Buffer) of
    full_md ->
      {Timestamp, BidAsk, Tail} = stockdb_format:decode_full_md(Buffer, State#dbstate.depth),

      {packet_from_mdentry(Timestamp, BidAsk, State),
        State#dbstate{last_timestamp = Timestamp, last_bidask = BidAsk, buffer = Tail}};
    delta_md ->
      {DTimestamp, DBidAsk, Tail} = stockdb_format:decode_delta_md(Buffer, State#dbstate.depth),
      BidAsk = bidask_delta_apply(State#dbstate.last_bidask, DBidAsk),
      Timestamp = State#dbstate.last_timestamp + DTimestamp,

      {packet_from_mdentry(Timestamp, BidAsk, State),
        State#dbstate{last_timestamp = Timestamp, last_bidask = BidAsk, buffer = Tail}}
  end.


split_bidask([Bid, Ask], _Depth) ->
  {Bid, Ask}.

packet_from_mdentry(Timestamp, BidAsk, #dbstate{depth = Depth, scale = Scale}) ->
  {Bid, Ask} = split_bidask(BidAsk, Depth),
  SBid = apply_scale(Bid, 1/Scale),
  SAsk = apply_scale(Ask, 1/Scale),
  {md, Timestamp, SBid, SAsk}.

apply_scale(PVList, Scale) when is_integer(Scale) ->
  lists:map(fun({Price, Volume}) ->
        {erlang:round(Price * Scale), Volume}
    end, PVList);

apply_scale(PVList, Scale) when is_float(Scale) ->
  lists:map(fun({Price, Volume}) ->
        {Price * Scale, Volume}
    end, PVList).
