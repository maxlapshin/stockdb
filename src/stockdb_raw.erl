%%% @doc Raw stockdb access library

-module(stockdb_raw).
-author({"Danil Zagoskin", z@gosk.in}).

-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("stockdb.hrl").

-export([open/2, restore_state/1, seek_utc/2, read_event/1, read_file/1, file_info/2, close/1]).
-export([foldl/3, foldl_range/4]).

-export([init_with_opts/1, read_packet_from_buffer/1]).
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


open(FileName, Options) ->
  {StockDBOpts, OpenOpts} = lists:partition(fun is_stockdb_option/1, Options),

  {Action, FileOpts} = determine_action(OpenOpts),
  FileExists = filelib:is_regular(FileName),

  case {Action, FileExists} of
    % {create, _} ->
    %   create_new_db(FileName, FileOpts, StockDBOpts);
    {read, _} ->
      open_existing_db(FileName, FileOpts, StockDBOpts)
    % {append, true} ->
    %   open_existing_db(FileName, FileOpts, StockDBOpts);
    % {append, false} ->
    %   create_new_db(FileName, FileOpts, StockDBOpts)
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

  StateReady = case lists:member(write, FileOpts) of
    true -> fast_forward(StateChunkRead);
    false -> StateChunkRead
  end,

  % ?D({last_packet, StateReady#dbstate.last_timestamp}),
  {ok, StateReady}.

restore_state(State) ->
  {ok, fast_forward(State)}.

update_db_options(OldOptions, _NewOptions) ->
  % TODO: decide what we can update
  OldOptions.

buffer_data(#dbstate{} = State) ->
  seek_utc(undefined, State#dbstate{buffer = undefined}).


%% @doc Set buffer to contain data from given UTC timestamp to EOF
seek_utc(UTC, #dbstate{file = File, chunk_map = ChunkMap, chunk_map_offset = ChunkMapOffset, chunk_size = ChunkSize,
    buffer = OldBuffer, buffer_end = OldBufEnd} = State) ->

  MinChunkStart = case UTC of
    undefined -> 0;
    Timestamp when is_integer(Timestamp) ->
      Timestamp - timer:seconds(ChunkSize)
  end,

  BufStart = case OldBuffer of
    <<_/binary>> when is_integer(OldBufEnd) ->
      OldBufEnd - erlang:byte_size(OldBuffer);
    _ ->
      undefined
  end,

  ChunksToRead = lists:dropwhile(fun({_N, T, _O}) -> T =< MinChunkStart end, ChunkMap),
  {Buffer, BufEnd} = case ChunksToRead of
    [] ->
      {<<>>, undefined};
    [{_N, _T, ROffset0}|_] when is_integer(BufStart) andalso BufStart =< ROffset0 ->
      % No need to access file -- just trim existing buffer
      SkipBytes = ROffset0 - BufStart,
      <<_:SkipBytes/binary, NewBuffer/binary>> = OldBuffer,
      {NewBuffer, OldBufEnd};
    [{_N, _T, ROffset0}|_] ->
      {ok, FileSize} = file:position(File, eof),
      Offset0 = ROffset0 + ChunkMapOffset,
      {ok, RealBuffer} = file:pread(File, Offset0, FileSize - Offset0),
      {RealBuffer, FileSize - ChunkMapOffset}
  end,
  drop_before(State#dbstate{buffer = Buffer, buffer_end = BufEnd}, UTC).

%% @doc Drop buffered packets until next one has later timestamp than given UTC
drop_before(State, undefined) ->
  State;
drop_before(State, UTC) ->
  case read_event(State) of
    {eof, NextState} ->
      % EOF. Just return what we have
      NextState;
    {Event, NextState} ->
      case packet_timestamp(Event) of
        Before when Before < UTC ->
          % Drop more
          drop_before(NextState, UTC);
        _After ->
          % Revert to state before getting event
          State
      end
  end.

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
  foldl_range(Fun, Acc0, FileName, {undefined, undefined}).

% foldl_range: fold over entries in specified time range
foldl_range(Fun, Acc0, FileName, {Start, End}) ->
  {ok, State0} = open(FileName, [read, binary]),
  State1 = seek_utc(Start, State0),
  FoldResult = case End of
    undefined ->
      do_foldl_full(Fun, Acc0, State1);
    _ ->
      do_foldl_until(Fun, Acc0, State1, End)
  end,
  close(State1),
  FoldResult.

do_foldl_full(Fun, AccIn, State) ->
  {Event, NextState} = read_event(State),
  case Event of
    eof ->
      % Finish folding -- no more events
      AccIn;
    _event ->
      % Iterate
      AccOut = Fun(Event, AccIn),
      do_foldl_full(Fun, AccOut, NextState)
  end.

do_foldl_until(Fun, AccIn, State, End) ->
  {Event, NextState} = read_event(State),
  case Event of
    eof ->
      % Finish folding -- no more events
      AccIn;
    _event ->
      case packet_timestamp(Event) of
        Large when Large > End ->
          % end of given interval
          AccIn;
        _small ->
          % Iterate
          AccOut = Fun(Event, AccIn),
          do_foldl_until(Fun, AccOut, NextState, End)
      end
  end.

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
  timer:hours(24) div timer:seconds(ChunkSize) + 1.


read_chunk_map(#dbstate{} = State) ->
  ChunkMap = lists:map(fun({Number, Offset}) ->
          try
            Timestamp = read_timestamp_at_offset(Offset, State),
            {Number, Timestamp, Offset}
          catch
            error:Reason ->
              ?D({error_reading_timestamp, Offset, Reason}),
              % FIXME: if opened for reading, use error_logger to inform and read, what is possible
              % In other case, validate file
              % write_chunk_offset(Number, 0, State),
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



utc_to_daystart(UTC) ->
  DayLength = timer:hours(24),
  DayTail = UTC rem DayLength,
  UTC - DayTail.


 

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
  LastState = case read_buffered_events(State#dbstate{buffer = Buffer, buffer_end = FileSize - ChunkMapOffset, next_md_full = true}) of
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
  ChunkSizeMs = timer:seconds(ChunkSize),

  LastState#dbstate{
    daystart = Daystart,
    next_chunk_time = Daystart + ChunkSizeMs * (N + 1)}.


read_timestamp_at_offset(Offset, #dbstate{file = File, chunk_map_offset = ChunkMapOffset}) ->
  {ok, Buffer} = file:pread(File, ChunkMapOffset + Offset, 8),
  stockdb_format:decode_timestamp(Buffer).


read_buffered_events(State) ->
  read_buffered_events(State, []).

read_buffered_events(#dbstate{} = State, RevEvents) ->
  try
    case read_event(State) of
      {eof, NewState} ->
        {lists:reverse(RevEvents), NewState};
      {Event, NewState} ->
        read_buffered_events(NewState, [Event|RevEvents])
    end
  catch
    error:Error ->
      ?D({parse_error, State#dbstate.buffer, Error, erlang:get_stacktrace()}),
      {parse_error, State, lists:reverse(RevEvents)}
  end.

%% @doc Read first event in buffer and return it (or eof) with modified state
read_event(#dbstate{buffer = undefined} = State) ->
  % No buffer. Fill it and retry
  read_event(buffer_data(State));

read_event(#dbstate{buffer = <<>>} = State) ->
  % Empty buffer. Return eof
  {eof, State};

read_event(State) ->
  % Buffer seems to be OK, read packet
  read_packet_from_buffer(State).

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
  SBid = if is_number(Scale) -> apply_scale(Bid, 1/Scale); true -> Bid end,
  SAsk = if is_number(Scale) -> apply_scale(Ask, 1/Scale); true -> Ask end,
  {md, Timestamp, SBid, SAsk}.

apply_scale(PVList, Scale) when is_float(Scale) ->
  lists:map(fun({Price, Volume}) ->
        {Price * Scale, Volume}
    end, PVList).



packet_timestamp({md, Timestamp, _Bid, _Ask}) -> Timestamp;
packet_timestamp({trade, Timestamp, _Price, _Volume}) -> Timestamp.
