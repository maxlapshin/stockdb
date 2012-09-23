-module(stockdb_appender).
-author('Max Lapshin <max@maxidoors.ru>').

-include("../include/stockdb.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("stockdb.hrl").
-include("log.hrl").


-export([open/2, append/2, close/1]).


open(Path, Opts) ->
  case filelib:is_regular(Path) of
    true ->
      open_existing_db(Path, Opts);
    false ->
      create_new_db(Path, Opts)
  end.


close(#dbstate{file = File} = State) ->
  write_candle(State),
  file:close(File),
  ok.


%% @doc Here we create skeleton for new DB
%% Structure of file is following:
%% #!/usr/bin/env stockdb
%% header: value
%% header: value
%% header: value
%%
%% chunkmap of fixed size
%% rows
create_new_db(Path, Opts) ->
  filelib:ensure_dir(Path),
  {ok, File} = file:open(Path, [binary,write,exclusive,raw]),
  {ok, 0} = file:position(File, bof),
  ok = file:truncate(File),

  {stock, Stock} = lists:keyfind(stock, 1, Opts),
  {date, Date} = lists:keyfind(date, 1, Opts),
  State = #dbstate{
    mode = append,
    version = ?STOCKDB_VERSION,
    stock = Stock,
    date = Date,
    sync = not lists:member(nosync, Opts),
    path = Path,
    have_candle = false,
    depth = proplists:get_value(depth, Opts, 1),
    scale = proplists:get_value(scale, Opts, 100),
    chunk_size = proplists:get_value(chunk_size, Opts, 5*60)
  },

  {ok, CandleOffset0} = write_header(File, State),
  CandleOffset = case State#dbstate.have_candle of
    true -> CandleOffset0;
    false -> undefined
  end,
  {ok, ChunkMapOffset} = write_candle(File, State),
  {ok, _CMSize} = write_chunk_map(File, State),

  {ok, State#dbstate{
      file = File,
      candle_offset = CandleOffset,
      chunk_map_offset = ChunkMapOffset
    }}.


open_existing_db(Path, _Opts) ->
  stockdb_reader:open_existing_db(Path, [binary,write,read,raw]).


event_timestamp(#md{timestamp = TS}) -> TS;
event_timestamp(#trade{timestamp = TS}) -> TS.

append(_Event, #dbstate{mode = Mode}) when Mode =/= append ->
  {error, reopen_in_append_mode};

append(Event, #dbstate{next_chunk_time = NCT, file = File, last_md = LastMD, sync = Sync} = State)
when is_record(Event, md) orelse is_record(Event, trade) ->
  Timestamp = event_timestamp(Event),
  if
    (Timestamp >= NCT orelse NCT == undefined) ->
      {ok, EOF} = file:position(File, eof),
      {ok, State_} = append_first_event(Event, State),
      if Sync -> file:sync(File); true -> ok end,
      {ok, State1_} = start_chunk(Timestamp, EOF, State_),
      if Sync -> file:sync(File); true -> ok end,
      {ok, State1_};
    LastMD == undefined andalso is_record(Event, md) ->
      append_full_md(Event, State);
    is_record(Event, md) ->
      append_delta_md(Event, State);
    is_record(Event, trade) ->
      append_trade(Event, State)
  end.

append_first_event(Event, State) when is_record(Event, md) ->
  append_full_md(Event, State);

append_first_event(Event, State) when is_record(Event, trade) ->
  append_trade(Event, State#dbstate{last_md = undefined}).


write_header(File, #dbstate{chunk_size = CS, date = Date, depth = Depth, scale = Scale, stock = Stock, version = Version,
  have_candle = HaveCandle}) ->
  StockDBOpts = [{chunk_size,CS},{date,Date},{depth,Depth},{scale,Scale},{stock,Stock},{version,Version},{have_candle,HaveCandle}],
  {ok, 0} = file:position(File, 0),
  ok = file:write(File, <<"#!/usr/bin/env stockdb\n">>),
  lists:foreach(fun
    ({have_candle,false}) ->
      ok;
    ({Key, Value}) ->
      ok = file:write(File, [io_lib:print(Key), ": ", stockdb_format:format_header_value(Key, Value), "\n"])
    end, StockDBOpts),
  ok = file:write(File, "\n"),
  file:position(File, cur).


write_candle(File, #dbstate{have_candle = true}) ->
  file:write(File, <<0:32, 0:32, 0:32, 0:32>>),
  file:position(File, cur);

write_candle(File, #dbstate{have_candle = false}) ->
  file:position(File, cur).

write_chunk_map(File, #dbstate{chunk_size = ChunkSize}) ->
  ChunkCount = ?NUMBER_OF_CHUNKS(ChunkSize),

  ChunkMap = [<<0:?OFFSETLEN>> || _ <- lists:seq(1, ChunkCount)],
  Size = ?OFFSETLEN * ChunkCount,

  ok = file:write(File, ChunkMap),
  {ok, Size}.



start_chunk(Timestamp, Offset, #dbstate{daystart = undefined, date = Date} = State) ->
  start_chunk(Timestamp, Offset, State#dbstate{daystart = daystart(Date)});

start_chunk(Timestamp, Offset, #dbstate{daystart = Daystart, chunk_size = ChunkSize,
    chunk_map = ChunkMap} = State) ->

  ChunkSizeMs = timer:seconds(ChunkSize),
  ChunkNumber = (Timestamp - Daystart) div ChunkSizeMs,

  % sanity check
  (Timestamp - Daystart) < timer:hours(24) orelse erlang:error({not_this_day, Timestamp}),

  ChunkOffset = current_chunk_offset(Offset, State),
  write_chunk_offset(ChunkNumber, ChunkOffset, State),

  NextChunkTime = Daystart + ChunkSizeMs * (ChunkNumber + 1),

  Chunk = {ChunkNumber, Timestamp, ChunkOffset},
  % ?D({new_chunk, Chunk}),
  State1 = State#dbstate{
    chunk_map = ChunkMap ++ [Chunk],
    next_chunk_time = NextChunkTime},
  write_candle(State1),
  {ok, State1}.


write_candle(#dbstate{have_candle = false}) ->  ok;
write_candle(#dbstate{candle = undefined}) -> ok;
write_candle(#dbstate{have_candle = true, candle_offset = CandleOffset, candle = {O,H,L,C}, file = File}) ->
  ok = file:pwrite(File, CandleOffset, <<1:1, O:31, H:32, L:32, C:32>>).



current_chunk_offset(Offset, #dbstate{chunk_map_offset = ChunkMapOffset} = _State) ->
  Offset - ChunkMapOffset.

write_chunk_offset(ChunkNumber, ChunkOffset, #dbstate{file = File, chunk_map_offset = ChunkMapOffset} = _State) ->
  ByteOffsetLen = ?OFFSETLEN div 8,
  ok = file:pwrite(File, ChunkMapOffset + ChunkNumber*ByteOffsetLen, <<ChunkOffset:?OFFSETLEN/integer>>).


append_full_md(#md{timestamp = Timestamp} = MD, #dbstate{depth = Depth, file = File, scale = Scale} = State) ->
  DepthSetMD = setdepth(MD, Depth),
  Data = stockdb_format:encode_full_md(DepthSetMD, Scale),
  {ok, _EOF} = file:position(File, eof),
  ok = file:write(File, Data),
  {ok, State#dbstate{
      last_timestamp = Timestamp,
      last_md = DepthSetMD}
  }.

append_delta_md(#md{timestamp = Timestamp} = MD, #dbstate{depth = Depth, file = File, last_md = LastMD, scale = Scale} = State) ->
  DepthSetMD = setdepth(MD, Depth),
  Data = stockdb_format:encode_delta_md(DepthSetMD, LastMD, Scale),
  {ok, _EOF} = file:position(File, eof),
  ok = file:write(File, Data),
  {ok, State#dbstate{
      last_timestamp = Timestamp,
      last_md = DepthSetMD}
  }.

append_trade(#trade{timestamp = Timestamp, price = Price} = Trade, 
  #dbstate{file = File, scale = Scale, candle = Candle, have_candle = HaveCandle} = State) ->
  Data = stockdb_format:encode_trade(Trade, Scale),
  {ok, _EOF} = file:position(File, eof),
  ok = file:write(File, Data),
  Candle1 = case HaveCandle of
    true -> candle(Candle, round(Price*Scale));
    false -> Candle
  end,
  {ok, State#dbstate{last_timestamp = Timestamp, candle = Candle1}}.


setdepth(#md{bid = Bid, ask = Ask} = MD, Depth) ->
  MD#md{
    bid = setdepth(Bid, Depth),
    ask = setdepth(Ask, Depth)};

setdepth(_Quotes, 0) ->
  [];
setdepth([], Depth) ->
  [{0, 0} || _ <- lists:seq(1, Depth)];
setdepth([Q|Quotes], Depth) ->
  [Q|setdepth(Quotes, Depth - 1)].


daystart(Date) ->
  DaystartSeconds = calendar:datetime_to_gregorian_seconds({Date, {0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  DaystartSeconds * 1000.


candle(undefined, Price) -> {Price, Price, Price, Price};
candle({O,H,L,_C}, Price) when Price > H -> {O,Price,L,Price};
candle({O,H,L,_C}, Price) when Price < L -> {O,H,Price,Price};
candle({O,H,L,_C}, Price) -> {O,H,L,Price}.



