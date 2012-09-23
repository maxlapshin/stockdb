%%% @doc stockdb_reader
%%% Read-only API

-module(stockdb_reader).
-author({"Danil Zagoskin", z@gosk.in}).

-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("stockdb.hrl").
-include("../include/stockdb.hrl").

% Open DB, read its contents and close,
% returning self-sufficient state
-export([open/1, open_for_migrate/1, open_existing_db/2]).

-export([file_info/1, file_info/2]).

open(Path) ->
  case filelib:is_regular(Path) of
    true ->
      {ok, State1} = open_existing_db(Path, [read, binary, raw]),
      State2 = #dbstate{file = F} = buffer_data(State1),
      file:close(F),
      {ok, State2#dbstate{file = undefined}};
    false ->
      {error, nofile}
  end.

open_for_migrate(Path) ->
  {ok, State1} = open_existing_db(Path, [migrate, read, binary, raw]),
  State2 = #dbstate{file = F} = buffer_data(State1),
  file:close(F),
  {ok, State2#dbstate{file = undefined}}.

open_existing_db(Path, Modes) ->
  {ok, File} = file:open(Path, Modes -- [migrate]),
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, AfterHeaderOffset} = read_header(File),

  {version, Version} = lists:keyfind(version, 1, SavedDBOpts),
  {stock, Stock} = lists:keyfind(stock, 1, SavedDBOpts),
  {date, Date} = lists:keyfind(date, 1, SavedDBOpts),
  {scale, Scale} = lists:keyfind(scale, 1, SavedDBOpts),
  {depth, Depth} = lists:keyfind(depth, 1, SavedDBOpts),
  {chunk_size, ChunkSize} = lists:keyfind(chunk_size, 1, SavedDBOpts),
  HaveCandle = proplists:get_value(have_candle, SavedDBOpts, false),

  {CandleOffset, ChunkMapOffset} = case HaveCandle of
    true -> {AfterHeaderOffset, AfterHeaderOffset + 4*4};
    false -> {undefined, AfterHeaderOffset}
  end,
  
  State0 = #dbstate{
    mode = append,
    version = Version,
    stock = Stock,
    date = Date,
    depth = Depth,
    scale = Scale,
    chunk_size = ChunkSize,
    file = File,
    path = Path,
    have_candle = HaveCandle,
    candle_offset = CandleOffset,
    chunk_map_offset = ChunkMapOffset
  },

  State2 = read_chunk_map(read_candle(State0)),
  case Version of
    ?STOCKDB_VERSION ->
      ValidatedState = stockdb_validator:validate(State2),
      {ok, ValidatedState};
    _Other ->
      case lists:member(migrate, Modes) of
        true ->
          {ok, State2};
        false ->
          erlang:error({need_to_migrate, Path})
      end
  end.



%% @doc read data from chunk map start to EOF
buffer_data(#dbstate{file = File, chunk_map_offset = ChunkMapOffset} = State) ->
  % determine file size
  {ok, FileSize} = file:position(File, eof),
  % read all data from data start to file end
  {ok, Buffer} = file:pread(File, ChunkMapOffset, FileSize - ChunkMapOffset),
  % return state with buffer set
  State#dbstate{buffer = Buffer}.


candle_info(undefined, _) -> [];
candle_info({O,H,L,C}, Scale) -> [{candle, {O/Scale,H/Scale,L/Scale,C/Scale}}].


%% @doc return some file_info about opened stockdb
file_info(#dbstate{stock = Stock, date = Date, path = Path, scale = Scale, candle = Candle}) ->
  candle_info(Candle,Scale) ++ [{path, Path},{stock, Stock}, {date, Date}];

file_info(FileName) ->
  file_info(FileName, [path, stock, date, version, scale, depth, candle]).

%% @doc read file info
file_info(FileName, Fields) ->
  case filelib:is_regular(FileName) of
    true -> get_file_info(FileName, Fields);
    false -> undefined
  end.

get_file_info(FileName, Fields) ->
  {ok, File} = file:open(FileName, [read, binary]),
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, AfterHeaderOffset} = read_header(File),
  {ChunkMapOffset,CandleOffset} = case proplists:get_value(have_candle,SavedDBOpts,false) of
    true -> {AfterHeaderOffset + 4*4, AfterHeaderOffset};
    false -> {AfterHeaderOffset, undefined}
  end,

  Result = lists:map(fun
      (presence) ->
        ChunkSize = proplists:get_value(chunk_size, SavedDBOpts),
        NZChunks = nonzero_chunks(#dbstate{file=File, chunk_map_offset = ChunkMapOffset, chunk_size = ChunkSize}),
        Presence = {?NUMBER_OF_CHUNKS(ChunkSize), [N || {N, _} <- NZChunks]},
        {presence, Presence};
      (candle) when CandleOffset == undefined->
        {candle, undefined};
      (candle) when is_number(CandleOffset) ->
        Scale = proplists:get_value(scale, SavedDBOpts),
        {candle, read_candle(File,CandleOffset,Scale)};
      (Field) ->
        Value = proplists:get_value(Field, [{path, FileName} | SavedDBOpts]),
        {Field, Value}
    end, Fields),

  file:close(File),
  Result.



%% @doc Read header from file descriptor, return list of key:value pairs and position at chunkmap start
read_header(File) ->
  Options = read_header_lines(File, []),
  {ok, Offset} = file:position(File, cur),
  {ok, Options, Offset}.

%% @doc Helper for read_header -- read lines until empty line is met
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

%% @doc Accept header line and return {Key, Value}, ignore (for comments) or stop
parse_header_line(HeaderLine) when is_binary(HeaderLine) ->
  % We parse strings, convert
  parse_header_line(erlang:binary_to_list(HeaderLine));

parse_header_line("#" ++ _Comment) ->
  % Comment. Ignore
  ignore;

parse_header_line("\n") ->
  % Empty line. Next byte is chunkmap
  stop;

parse_header_line(HeaderLine) when is_list(HeaderLine) ->
  % Remove trailing newline
  parse_header_line(string:strip(HeaderLine, right, $\n), nonewline).

parse_header_line(HeaderLine, nonewline) ->
  % Extract key and value
  [KeyRaw, ValueRaw] = string:tokens(HeaderLine, ":"),

  KeyStr = string:strip(KeyRaw, both),
  ValueStr = string:strip(ValueRaw, both),

  Key = erlang:list_to_atom(KeyStr),
  Value = stockdb_format:parse_header_value(Key, ValueStr),

  {Key, Value}.


%% @doc read candle from file descriptor
read_candle(#dbstate{have_candle = false} = State) ->
  State;

read_candle(#dbstate{file = File, have_candle = true, candle_offset = CandleOffset} = State) ->
  State#dbstate{candle = read_candle(File,CandleOffset)}.

read_candle(File, CandleOffset) ->
  case file:pread(File, CandleOffset, 4*4) of
    {ok, <<0:1, _O:31, _H:32, _L:32, _C:32>>} -> undefined;
    {ok, <<1:1, O:31, H:32, L:32, C:32>>} -> {O,H,L,C}
  end.

read_candle(File, CandleOffset, Scale) ->
  case read_candle(File, CandleOffset) of
    undefined -> undefined;
    {O,H,L,C} -> {O/Scale,H/Scale,L/Scale,C/Scale}
  end.


%% @doc Read chunk map and validate corresponding timestamps.
%% Result is saved to state
read_chunk_map(#dbstate{} = State) ->
  NonZeroChunks = nonzero_chunks(State),
  ChunkMap = [{Number, read_timestamp_at_offset(Offset, State), Offset} || {Number, Offset} <- NonZeroChunks],
  State#dbstate{chunk_map = ChunkMap}.

%% @doc Read raw chunk map and return {Number, Offset} list for chunks containing data
nonzero_chunks(#dbstate{file = File, chunk_size = ChunkSize, chunk_map_offset = ChunkMapOffset}) ->
  ChunkCount = ?NUMBER_OF_CHUNKS(ChunkSize),
  {ok, ChunkMap} = file:pread(File, ChunkMapOffset, ChunkCount*?OFFSETLEN div 8),
  Chunks1 = lists:zip(lists:seq(0,ChunkCount - 1), [Offset || <<Offset:?OFFSETLEN>> <= ChunkMap]),
  [{N,Offset} || {N,Offset} <- Chunks1, Offset =/= 0].

  

%% @doc Read timestamp at specified offset
read_timestamp_at_offset(Offset, #dbstate{buffer = undefined, file = File, chunk_map_offset = ChunkMapOffset}) ->
  {ok, Header} = file:pread(File, ChunkMapOffset + Offset, 8),
  stockdb_format:get_timestamp(Header);

read_timestamp_at_offset(Offset, #dbstate{buffer = Buffer}) ->
  <<_:Offset/binary, Bin/binary>> = Buffer,
  stockdb_format:get_timestamp(Bin).
