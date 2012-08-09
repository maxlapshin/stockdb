%%% @doc stockdb_reader
%%% Read-only API

-module(stockdb_reader).
-author({"Danil Zagoskin", z@gosk.in}).

-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("stockdb.hrl").

% Open DB, read its contents and close,
% returning self-sufficient state
-export([open/1, open_existing_db/2]).

open(Path) ->
  case filelib:is_regular(Path) of
    true ->
      {State0, _} = open_existing_db(Path, [read, binary, raw]),
      State1 = buffer_data(State0),
      close(State1);
    false ->
      {error, nofile}
  end.

open_existing_db(Path, Modes) ->
  {ok, File} = file:open(Path, Modes),
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, ChunkMapOffset} = read_header(File),

  {version, Version} = lists:keyfind(version, 1, SavedDBOpts),
  Version == ?STOCKDB_VERSION orelse erlang:error({need_to_migrate, Path}),
  {stock, Stock} = lists:keyfind(stock, 1, SavedDBOpts),
  {date, Date} = lists:keyfind(date, 1, SavedDBOpts),
  {scale, Scale} = lists:keyfind(scale, 1, SavedDBOpts),
  {depth, Depth} = lists:keyfind(depth, 1, SavedDBOpts),
  {chunk_size, ChunkSize} = lists:keyfind(chunk_size, 1, SavedDBOpts),
  
  State0 = #dbstate{
    mode = append,
    version = Version,
    stock = Stock,
    date = Date,
    depth = Depth,
    scale = Scale,
    chunk_size = ChunkSize,
    file = File,
    chunk_map_offset = ChunkMapOffset
  },

  {_StateChunkRead, _BadChunks} = read_chunk_map(State0).


%% @doc read data from chunk map start to EOF
buffer_data(State) ->
  State.

%% @doc Close file to make state self-sufficient
close(#dbstate{file = File} = State) ->
  ok = file:close(File),
  {ok, State#dbstate{file = undefined}}.


%% @doc read file info
file_info(FileName, Fields) ->
  {ok, File} = file:open(FileName, [read, binary]),
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, ChunkMapOffset} = read_header(File),

  Result = lists:map(fun
      (presence) ->
        ChunkSize = proplists:get_value(chunk_size, SavedDBOpts),
        NZChunks = nonzero_chunks(#dbstate{file=File, chunk_map_offset = ChunkMapOffset, chunk_size = ChunkSize}),
        {stockdb_raw:number_of_chunks(ChunkSize), [N || {N, _} <- NZChunks]};
      (Field) ->
        proplists:get_value(Field, SavedDBOpts)
    end, Fields),

  file:close(File),
  Result.

%% @doc Read header from file descriptor, return list of key:value pairs and position at chunkmap start
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


%% @doc Read chunk map and validate corresponding timestamps.
%% Result is saved to state
read_chunk_map(#dbstate{} = State) ->
  NonZeroChunks = nonzero_chunks(State),
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
  end, NonZeroChunks),
  {GoodChunkMap, BadChunks} = lists:partition(fun({_Number, Timestamp, Offset}) ->
    Timestamp * 1 > 0 andalso Offset * 1 > 0
  end, ChunkMap),
  {State#dbstate{chunk_map = GoodChunkMap}, BadChunks}.

%% @doc Read raw chunk map and return {Number, Offset} list for chunks containing data
nonzero_chunks(#dbstate{file = File, chunk_size = ChunkSize, chunk_map_offset = ChunkMapOffset}) ->
  ChunkCount = stockdb_raw:number_of_chunks(ChunkSize),
  {ok, ChunkMap} = file:pread(File, ChunkMapOffset, ChunkCount*?OFFSETLEN div 8),
  is_binary(ChunkMap) andalso size(ChunkMap) == ChunkCount*?OFFSETLEN div 8 orelse erlang:error(broken_database_file),
  Chunks1 = lists:zip(lists:seq(0,ChunkCount - 1), [Offset || <<Offset:?OFFSETLEN>> <= ChunkMap]),
  [{N,Offset} || {N,Offset} <- Chunks1, Offset =/= 0].

%% @doc Read timestamp at specified offset
read_timestamp_at_offset(Offset, #dbstate{file = File, chunk_map_offset = ChunkMapOffset}) ->
  {ok, Buffer} = file:pread(File, ChunkMapOffset + Offset, 8),
  stockdb_format:decode_timestamp(Buffer).
