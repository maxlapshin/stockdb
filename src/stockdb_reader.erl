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
-export([read_file/1]).

-export([file_info/2]).

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

  {ok, SavedDBOpts, ChunkMapOffset} = read_header(File),

  {version, Version} = lists:keyfind(version, 1, SavedDBOpts),
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
    path = Path,
    chunk_map_offset = ChunkMapOffset
  },

  State1 = read_chunk_map(State0),
  case Version of
    ?STOCKDB_VERSION ->
      ValidatedState = stockdb_validator:validate(State1),
      {ok, ValidatedState};
    _Other ->
      case lists:member(migrate, Modes) of
        true ->
          {ok, State1};
        false ->
          erlang:error({need_to_migrate, Path})
      end
  end.



read_file(#dbstate{buffer = Buffer, depth = Depth, chunk_map = [{_, _, Offset}|_], scale = Scale}) ->
  <<_:Offset/binary, Data/binary>> = Buffer,
  {ok, read_all_events(Data, Depth, undefined, Scale)};

read_file(#dbstate{chunk_map = []}) ->
  {ok, []};

read_file(Path) when is_list(Path) ->
  case open(Path) of
    {ok, #dbstate{} = State} ->
      read_file(State);
    Else ->
      Else
  end.

read_all_events(<<>>, _Depth, _PrevMD, _Scale) ->
  [];

read_all_events(Data, Depth, PrevMD, Scale) ->
  case stockdb_format:read_one_row(Data, Depth, PrevMD, Scale) of
    {ok, #md{} = MD, Rest} ->
      [MD|read_all_events(Rest, Depth, MD, Scale)];
    {ok, #trade{timestamp = TS} = Trade, Rest} ->
      NewMD = case PrevMD of
        undefined -> undefined;
        _ -> PrevMD#md{timestamp = TS}
      end,
      [Trade|read_all_events(Rest, Depth, NewMD, Scale)]
  end.

%% @doc read data from chunk map start to EOF
buffer_data(#dbstate{file = File, chunk_map_offset = ChunkMapOffset} = State) ->
  % determine file size
  {ok, FileSize} = file:position(File, eof),
  % read all data from data start to file end
  {ok, Buffer} = file:pread(File, ChunkMapOffset, FileSize - ChunkMapOffset),
  % return state with buffer set
  State#dbstate{buffer = Buffer}.


%% @doc read file info
file_info(FileName, Fields) ->
  case filelib:is_regular(FileName) of
    true -> get_file_info(FileName, Fields);
    false -> undefined
  end.

get_file_info(FileName, Fields) ->
  {ok, File} = file:open(FileName, [read, binary]),
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, ChunkMapOffset} = read_header(File),

  Result = lists:map(fun
      (presence) ->
        ChunkSize = proplists:get_value(chunk_size, SavedDBOpts),
        NZChunks = nonzero_chunks(#dbstate{file=File, chunk_map_offset = ChunkMapOffset, chunk_size = ChunkSize}),
        {?NUMBER_OF_CHUNKS(ChunkSize), [N || {N, _} <- NZChunks]};
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
  {ok, <<1:1, _:1/integer, Timestamp:62/integer>>} = file:pread(File, ChunkMapOffset + Offset, 8),
  Timestamp;

read_timestamp_at_offset(Offset, #dbstate{buffer = Buffer}) ->
  <<_:Offset/binary, 1:1, _:1/integer, Timestamp:62/integer, _/binary>> = Buffer,
  Timestamp.
