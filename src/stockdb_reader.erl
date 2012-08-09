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

-export([file_info/2]).

open(Path) ->
  case filelib:is_regular(Path) of
    true ->
      {State0, _} = open_existing_db(Path, [read, binary, raw]),
      close(State0);
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

  StateBuffered = buffer_data(State0),
  {_StateChunkRead, _BadChunks} = read_chunk_map(StateBuffered).


%% @doc read data from chunk map start to EOF
buffer_data(#dbstate{file = File, chunk_map_offset = ChunkMapOffset} = State) ->
  % determine file size
  {ok, FileSize} = file:position(File, eof),
  % read all data from data start to file end
  {ok, Buffer} = file:pread(File, ChunkMapOffset, FileSize - ChunkMapOffset),
  % return state with buffer set
  State#dbstate{buffer = Buffer}.

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


%% @doc Read chunk map and validate corresponding timestamps.
%% Result is saved to state
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
  {GoodChunkMap, BadChunks} = lists:partition(fun({_Number, Timestamp, Offset}) ->
    Timestamp * 1 > 0 andalso Offset * 1 > 0
  end, ChunkMap),
  {State#dbstate{chunk_map = GoodChunkMap}, BadChunks}.

%% @doc Read raw chunk map and return {Number, Offset} list for chunks containing data
nonzero_chunks(#dbstate{buffer = Buffer, chunk_size = ChunkSize}) ->
  ChunkCount = stockdb_raw:number_of_chunks(ChunkSize),
  {Chunks, _} = lists:mapfoldl(fun(Number, Bin) ->
        <<Offset:?OFFSETLEN/integer, Tail/binary>> = Bin,
        Chunk = {Number, Offset},
        {Chunk, Tail}
    end, Buffer, lists:seq(0, ChunkCount - 1)),
  [{N,Offset} || {N,Offset} <- Chunks, Offset =/= 0].

%% @doc Read timestamp at specified offset
read_timestamp_at_offset(Offset, #dbstate{buffer = Buffer}) ->
  <<_:Offset/binary, SubBuf/binary>> = Buffer,
  stockdb_format:decode_timestamp(SubBuf).
