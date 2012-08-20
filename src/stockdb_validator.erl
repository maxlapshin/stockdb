-module(stockdb_validator).
-include("stockdb.hrl").
-include("log.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").


-export([validate/1]).


validate(#dbstate{path = Path, chunk_map = [], chunk_map_offset = ChunkMapOffset, chunk_size = ChunkSize} = State) ->
  ChunkCount = ?NUMBER_OF_CHUNKS(ChunkSize),
  ChunkMapSize = ChunkCount*?OFFSETLEN div 8,
  GoodFileSize = ChunkMapOffset + ChunkMapSize,
  case file:read_file_info(Path) of
    {ok, #file_info{size = Size}} when Size > GoodFileSize ->
      error_logger:error_msg("Empty database ~s is longer than required size, truncating all records~n", [Path]),
      {ok, F} = file:open(Path, [write,read,binary,raw]),
      file:position(F, GoodFileSize),
      file:truncate(F),
      file:close(F),
      State;
    {ok, #file_info{size = Size}} when Size < GoodFileSize ->
      error_logger:error_msg("Empty database ~s is shorter and have broken chunk map, delete it~n", [Path]),
      State;
    {ok, #file_info{size = GoodFileSize}} ->
      State
  end;
  

validate(#dbstate{path = Path, file = File, chunk_map = ChunkMap, chunk_map_offset = ChunkMapOffset, chunk_size = ChunkSize} = State) ->
  {Number, Timestamp, Offset} = lists:last(ChunkMap),
  {ok, #file_info{size = Size}} = file:read_file_info(Path),
  AbsOffset = ChunkMapOffset + Offset,
  {ok, LastChunk} = file:pread(File, AbsOffset, Size),
  
  State1 = case validate_chunk(LastChunk, 0, State) of
    {ok, State1_} ->
      State1_;
    {error, State1_, BadOffset} ->
      error_logger:error_msg("Database ~s is broken at offset ~B, truncating~n", [Path, BadOffset]),
      {ok, F} = file:open(Path, [write,read,binary,raw]),
      file:position(F, AbsOffset + BadOffset),
      file:truncate(F),
      file:close(F),
      State1_
  end,
  
  Daystart = utc_to_daystart(Timestamp),

  State1#dbstate{
    daystart = Daystart,
    next_chunk_time = Daystart + timer:seconds(ChunkSize) * (Number + 1)
  }.
  
utc_to_daystart(UTC) ->
  DayLength = timer:hours(24),
  DayTail = UTC rem DayLength,
  UTC - DayTail.


validate_chunk(<<>>, _, State) ->
  {ok, State};

validate_chunk(Chunk, Offset, #dbstate{last_md = MD, depth = Depth, scale = Scale} = State) ->
  % ?debugFmt("decode_packet ~B/~B ~B ~B ~p", [Offset, size(Chunk),Depth, Scale, MD]),
  case stockdb_format:decode_packet(Chunk, Depth, MD, Scale) of
    {ok, {md, TS, _Bid, _Ask} = NewMD, Size} ->
      <<_:Size/binary, Rest/binary>> = Chunk,
      validate_chunk(Rest, Offset + Size, State#dbstate{last_md = NewMD, last_timestamp = TS});
    {ok, {trade, TS, _, _}, Size} ->
      <<_:Size/binary, Rest/binary>> = Chunk,
      validate_chunk(Rest, Offset + Size, State#dbstate{last_timestamp = TS});
    {error, _Reason} ->
      {error, State, Offset}
  end.
