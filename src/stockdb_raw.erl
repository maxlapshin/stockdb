%%% @doc Raw stockdb access library

-module(stockdb_raw).
-author({"Danil Zagoskin", z@gosk.in}).

-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([open/2, read_file/1, file_info/2, append/2, close/1]).
-export([foldl/3]).

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
    daystart,
    next_md_full = true
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

  % ?D({last_packet, StateReady#dbstate.last_timestamp}),
  {ok, StateReady}.


update_db_options(OldOptions, _NewOptions) ->
  % TODO: decide what we can update
  OldOptions.


read_file(FileName) ->
  {ok, State0} = open(FileName, [read, binary]),
  {ok, FileSize} = file:position(State0#dbstate.file, eof),
  [{_, ROffset0}|_] = nonzero_chunks(State0),

  Offset0 = State0#dbstate.chunk_map_offset + ROffset0,
  {ok, Buffer} = file:pread(State0#dbstate.file, Offset0, FileSize - Offset0),
  close(State0),

  {Events, _State1} = read_buffered_events(State0#dbstate{buffer = Buffer}),
  {ok, Events}.

% Foldl: low-memory fold over entries
foldl(Fun, Acc0, FileName) ->
  {ok, State0} = open(FileName, [read, binary]),
  File = State0#dbstate.file,
  {ok, FileSize} = file:position(File, eof),

  ChunkROffsets = [Offset || {_Number, Offset} <- nonzero_chunks(State0)],

  % Get start of data and list of chunk ends
  % Note: this will always work, even if there are no chunks
  [FirstROffset|ChunkEnds] = ChunkROffsets ++ [FileSize],

  % Get chunk sizes for easy read
  {ChunkSizes, _} = lists:mapfoldl(fun(Offset, PrevOffset) ->
        Size = Offset - PrevOffset,
        {Size, Offset}
    end, FirstROffset, ChunkEnds),

  % Go to first chunk start
  file:position(File, State0#dbstate.chunk_map_offset + FirstROffset),

  % Fold over chunks
  FoldResult = lists:foldl(fun(ChunkSize, AccIn) ->
        {ok, Buffer} = file:read(File, ChunkSize),
        % We don't need fresh state at chunk start, so drop modified one
        {Events, _State1} = read_buffered_events(State0#dbstate{buffer = Buffer}),
        % Do partial foldl on this chunk
        _AccOut = lists:foldl(Fun, AccIn, Events)
    end, Acc0, ChunkSizes),

  close(State0),
  FoldResult.


file_info(FileName, Fields) ->
  {ok, File} = file:open(FileName, [read, binary]),
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, ChunkMapOffset} = read_header(File),

  Result = lists:map(fun
      (presence) ->
        ChunkSize = proplists:get_value(chunk_size, SavedDBOpts),
        NZChunks = nonzero_chunks(#dbstate{file=File, chunk_map_offset = ChunkMapOffset, chunk_size = ChunkSize}),
        {number_of_chunks(ChunkSize), [N || {N, _} <- NZChunks]};
      (Field) ->
        proplists:get_value(Field, SavedDBOpts)
    end, Fields),

  file:close(File),
  Result.


close(#dbstate{file = File} = _State) ->
  file:close(File).


append({trade, Timestamp, _ ,_} = Trade, #dbstate{next_chunk_time = NCT} = State) when Timestamp >= NCT ->
  append(Trade, start_chunk(Timestamp, State));

append({md, Timestamp, _, _} = MD, #dbstate{next_chunk_time = NCT} = State) when Timestamp >= NCT ->
  append(MD, start_chunk(Timestamp, State));


append({trade, Timestamp, Price, Volume}, #dbstate{scale = Scale} = State) ->
  StorePrice = erlang:round(Price * Scale),
  append_trade({trade, Timestamp, StorePrice, Volume}, State);

append({md, _Timestamp, _Bid, _Ask} = MD, #dbstate{scale = Scale, next_md_full = true} = State) ->
  append_full_md(scale_md(MD, Scale), State);

append({md, _Timestamp, _Bid, _Ask} = MD, #dbstate{scale = Scale} = State) ->
  append_delta_md(scale_md(MD, Scale), State).


write_header(File, StockDBOpts) ->
  {ok, 0} = file:position(File, 0),
  ok = file:write(File, <<"#!/usr/bin/env stockdb\n">>),
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
          try
            Timestamp = read_timestamp_at_offset(Offset, State),
            {Number, Timestamp, Offset}
          catch
            error:Reason ->
              ?D({error_reading_timestamp, Offset, Reason}),
              write_chunk_offset(Number, 0, State),
              {Number, 0, 0}
          end
    end, nonzero_chunks(State)),
  GoodChunkMap = lists:filter(fun({_Number, Timestamp, Offset}) ->
          Timestamp * 1 > 0 andalso Offset * 1 > 0
      end, ChunkMap),
  State#dbstate{chunk_map = GoodChunkMap}.


nonzero_chunks(#dbstate{file = File, chunk_size = ChunkSize, chunk_map_offset = ChunkMapOffset}) ->
  OffsetByteSize = ?OFFSETLEN div 8,
  ChunkCount = number_of_chunks(ChunkSize),
  ReversedResult = lists:foldl(fun(N, NZChunks) ->
        case file:pread(File, ChunkMapOffset + OffsetByteSize*N, OffsetByteSize) of
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
    chunk_map = ChunkMap} = State,

  ChunkSizeMs = timer:?CHUNKUNITS(ChunkSize),
  ChunkNumber = (Timestamp - Daystart) div ChunkSizeMs,

  % sanity check
  (Timestamp - Daystart) < timer:hours(24) orelse erlang:error({not_this_day, Timestamp}),

  ChunkOffset = current_chunk_offset(State),
  write_chunk_offset(ChunkNumber, ChunkOffset, State),

  NextChunkTime = Daystart + ChunkSizeMs * (ChunkNumber + 1),

  Chunk = {ChunkNumber, Timestamp, ChunkOffset},
  % ?D({new_chunk, Chunk}),
  State#dbstate{
    chunk_map = ChunkMap ++ [Chunk],
    next_chunk_time = NextChunkTime,
    next_md_full = true}.
 
current_chunk_offset(#dbstate{file = File, chunk_map_offset = ChunkMapOffset} = _State) ->
  {ok, EOF} = file:position(File, eof),
  _ChunkOffset = EOF - ChunkMapOffset.

write_chunk_offset(ChunkNumber, ChunkOffset, #dbstate{file = File, chunk_map_offset = ChunkMapOffset} = _State) ->
  ByteOffsetLen = ?OFFSETLEN div 8,
  ok = file:pwrite(File, ChunkMapOffset + ChunkNumber*ByteOffsetLen, <<ChunkOffset:?OFFSETLEN/integer>>).


append_full_md({md, Timestamp, Bid, Ask}, #dbstate{depth = Depth, file = File} = State) ->
  BidAsk = [setdepth(Bid, Depth), setdepth(Ask, Depth)],
  Data = stockdb_format:encode_full_md(Timestamp, BidAsk),
  {ok, _EOF} = file:position(File, eof),
  ok = file:write(File, Data),
  {ok, State#dbstate{
      last_timestamp = Timestamp,
      last_bidask = BidAsk,
      next_md_full = false}
  }.

append_delta_md({md, Timestamp, Bid, Ask}, #dbstate{depth = Depth, file = File, last_timestamp = LastTS, last_bidask = LastBA} = State) ->
  BidAsk = [setdepth(Bid, Depth), setdepth(Ask, Depth)],
  BidAskDelta = bidask_delta(LastBA, BidAsk),
  Data = stockdb_format:encode_delta_md(Timestamp - LastTS, BidAskDelta),
  {ok, _EOF} = file:position(File, eof),
  ok = file:write(File, Data),
  {ok, State#dbstate{
      last_timestamp = Timestamp,
      last_bidask = BidAsk}
  }.

append_trade({trade, Timestamp, Price, Volume}, #dbstate{file = File} = State) ->
  Data = stockdb_format:encode_trade(Timestamp, Price, Volume),
  {ok, _EOF} = file:position(File, eof),
  ok = file:write(File, Data),
  {ok, State#dbstate{last_timestamp = Timestamp}}.


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

  {ok, Buffer} = file:pread(File, AbsOffset, FileSize - AbsOffset),
  LastState = case read_buffered_events(State#dbstate{buffer = Buffer, next_md_full = true}) of
    {_Packets, OKState} ->
      OKState;

    {parse_error, FailState, _Packets} ->
      % Try to truncate erroneous tail
      BufferLen = erlang:byte_size(Buffer),
      ErrorLen = erlang:byte_size(FailState#dbstate.buffer),
      ?D({truncating_last_bytes, ErrorLen}),
      % Calculate position relative to chunk start
      GoodBufLen = BufferLen - ErrorLen,
      % Set position
      {ok, _} = file:position(File, AbsOffset + GoodBufLen),
      % Truncate. It will fail on read-only file, but it is OK
      ok == file:truncate(File) orelse erlang:throw({truncate_failed, possible_read_only}),
      FailState#dbstate{buffer = undefined}
  end,

  Daystart = utc_to_daystart(LastChunkTimestamp),
  ChunkSizeMs = timer:?CHUNKUNITS(ChunkSize),

  LastState#dbstate{
    daystart = Daystart,
    next_chunk_time = Daystart + ChunkSizeMs * (N + 1)}.


read_timestamp_at_offset(Offset, #dbstate{file = File, chunk_map_offset = ChunkMapOffset}) ->
  {ok, Buffer} = file:pread(File, ChunkMapOffset + Offset, 8),
  stockdb_format:decode_timestamp(Buffer).


read_buffered_events(State) ->
  read_buffered_events(State, []).

read_buffered_events(#dbstate{buffer = <<>>} = State, RevEvents) ->
  {lists:reverse(RevEvents), State};
read_buffered_events(#dbstate{} = State, RevEvents) ->
  try
    {Event, NewState} = read_packet_from_buffer(State),
    read_buffered_events(NewState, [Event|RevEvents])
  catch
    error:Error ->
      ?D({parse_error, State#dbstate.buffer, Error}),
      {parse_error, State, lists:reverse(RevEvents)}
  end.

read_packet_from_buffer(#dbstate{buffer = Buffer, depth = Depth, last_bidask = LastBidAsk, last_timestamp = LastTimestamp, scale = Scale} = State) ->
  case stockdb_format:packet_type(Buffer) of
    full_md ->
      {Timestamp, BidAsk, Tail} = stockdb_format:decode_full_md(Buffer, Depth),

      {packet_from_mdentry(Timestamp, BidAsk, State),
        State#dbstate{last_timestamp = Timestamp, last_bidask = BidAsk, buffer = Tail, next_md_full = false}};
    delta_md ->
      {DTimestamp, DBidAsk, Tail} = stockdb_format:decode_delta_md(Buffer, Depth),
      BidAsk = bidask_delta_apply(LastBidAsk, DBidAsk),
      Timestamp = LastTimestamp + DTimestamp,

      {packet_from_mdentry(Timestamp, BidAsk, State),
        State#dbstate{last_timestamp = Timestamp, last_bidask = BidAsk, buffer = Tail}};
    trade ->
      {Timestamp, Price, Volume, Tail} = stockdb_format:decode_trade(Buffer),
      {{trade, Timestamp, Price/Scale, Volume},
        State#dbstate{last_timestamp = Timestamp, buffer = Tail}}
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


scale_md({md, Timestamp, Bid, Ask}, Scale) ->
  SBid = apply_scale(Bid, Scale),
  SAsk = apply_scale(Ask, Scale),
  {md, Timestamp, SBid, SAsk}.
