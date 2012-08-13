%%% @doc migrate v1 to v2
-module(stockdb_migrate).

-include("stockdb.hrl").

-export([file/1]).


file(Filename) ->
  {ok, State0} = stockdb_reader:open_for_migrate(Filename),
  case State0#dbstate.version of
    1 ->
      migrate_from_v1(State0);
    ?STOCKDB_VERSION ->
      {error, latest}
  end.

migrate_from_v1(#dbstate{stock = Stock, date = Date, depth = Depth, scale = Scale, chunk_size = ChunkSize, buffer = Buffer} = FromState) ->
  DataStart = first_chunk_offset(FromState),
  <<_:DataStart/binary, DataBuffer/binary>> = Buffer,
  Events = stockdb_format_v1:read_all_packets(DataBuffer, FromState),
  NewPath = stockdb_fs:path(Stock, Date),
  file:delete(NewPath),
  {ok, Appender} = stockdb:open_append(Stock, Date, [{depth, Depth}, {scale, Scale}, {chunk_size, ChunkSize}]),
  LastAppender = lists:foldl(fun(E, A) ->
        {ok, NA} = stockdb:append(E, A),
        NA
    end, Appender, Events),
  ok = stockdb:close(LastAppender),
  {ok, NewPath}.

%% @doc get start of first chunk
first_chunk_offset(#dbstate{chunk_map = []} = _DBstate) ->
  % Empty chunk map -> offset undefined
  undefined;
first_chunk_offset(#dbstate{chunk_map = [{_N, _T, Offset}|_Rest]} = _DBstate) ->
  % Just return offset from first chunk
  Offset.
