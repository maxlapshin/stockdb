%%% @doc StockDB iterator module
%%% It accepts stockdb state and operates only with its buffer

-module(stockdb_iterator).
-author({"Danil Zagoskin", z@gosk.in}).

-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("stockdb.hrl").
-include("../include/stockdb.hrl").

% Create new iterator from stockdb state
-export([init/1]).

% Apply filter
-export([filter/2, filter/3]).

% Limit range
-export([seek_utc/2, set_range/2]).

% Access buffer
-export([read_event/1, all_events/1]).

% Restore last state
-export([restore_last_state/1]).

% May be useful
-export([foldl/3, foldl/4]).

-record(iterator, {
    dbstate,
    data_start,
    position,
    last_utc
  }).

-record(filter, {
    source,
    ffun,
    state,
    buffer = []
  }).

%% @doc Initialize iterator. Position at the very beginning of data
init(#dbstate{} = DBState) ->
  DataStart = first_chunk_offset(DBState),
  {ok, #iterator{
      dbstate = DBState,
      data_start = DataStart,
      position = DataStart}}.

%% @doc Filter source iterator, expposing same API as usual iterator
filter(Source, FilterFun) ->
  filter(Source, FilterFun, undefined).
filter(Source, FilterFun, State0) when is_atom(FilterFun) ->
  filter(Source, fun stockdb_filters:FilterFun/2, State0);
filter(Source, FilterFun, State0) when is_function(FilterFun, 2) ->
  create_filter(Source, FilterFun, State0).

create_filter(Source, FilterFun, State0) when is_record(Source, iterator) orelse is_record(Source, filter) ->
  #filter{
    source = Source,
    ffun = FilterFun,
    state = State0}.

%% @doc replay last chunk and return finl state
restore_last_state(Iterator) ->
  #iterator{dbstate = LastState} = seek_utc(eof, Iterator),
  % Drop buffer to free memory
  LastState#dbstate{buffer = undefined}.


%% @doc get start of first chunk
first_chunk_offset(#dbstate{chunk_map = []} = _DBstate) ->
  % Empty chunk map -> offset undefined
  undefined;
first_chunk_offset(#dbstate{chunk_map = [{_N, _T, Offset}|_Rest]} = _DBstate) ->
  % Just return offset from first chunk
  Offset.

%% @doc Set position to given time
seek_utc(Time, #iterator{data_start = DataStart, dbstate = #dbstate{chunk_map = ChunkMap, date = Date}} = Iterator) ->
  UTC = time_to_utc(Time, Date),
  ChunksBefore = case UTC of
    undefined -> [];
    eof -> ChunkMap;
    Int when is_integer(Int) -> lists:takewhile(fun({_N, T, _O}) -> T =< UTC end, ChunkMap)
  end,
  {_N, _T, ChunkOffset} = case ChunksBefore of
    [] -> {-1, -1, DataStart};
    [_|_] -> lists:last(ChunksBefore)
  end,
  seek_until(UTC, Iterator#iterator{position = ChunkOffset});

seek_utc(UTC, #filter{source = Source} = Filter) ->
  % For filter, seek in underlying source
  Filter#filter{source = seek_utc(UTC, Source)}.


set_last_utc(Time, #iterator{dbstate = #dbstate{date = Date}} = Iterator) ->
  UTC = time_to_utc(Time, Date),
  Iterator#iterator{last_utc = UTC};

set_last_utc(UTC, #filter{source = Source} = Filter) ->
  % For filter, set last_utc on underlying source
  Filter#filter{source = set_last_utc(UTC, Source)}.

time_to_utc({_H, _M, _S} = Time, Date) ->
  Seconds = calendar:datetime_to_gregorian_seconds({Date, Time}) - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
  Seconds * 1000;
time_to_utc(Time, _Date) ->
  Time.


%% @doc set time range
set_range({Start, End}, Iterator) ->
  set_last_utc(End, seek_utc(Start, Iterator)).


%% @doc Seek forward event-by-event while timestamp is less than given
seek_until(undefined, Iterator) ->
  Iterator;
seek_until(UTC, #iterator{} = Iterator) ->
  case read_event(Iterator) of
    {eof, NextIterator} ->
      % EOF. Just return what we have
      NextIterator;
    {Event, NextIterator} when is_integer(UTC) ->
      case packet_timestamp(Event) of
        Before when Before < UTC ->
          % Drop more
          seek_until(UTC, NextIterator);
        _After ->
          % Revert to state before getting event
          Iterator
      end;
    {_, NextIterator} when UTC == eof ->
      seek_until(UTC, NextIterator)
  end.

%% @doc Pop first event from iterator, return {Event|eof, NewIterator}
read_event(#iterator{position = undefined} = Iterator) ->
  {eof, Iterator};

read_event(#iterator{dbstate = #dbstate{buffer = FullBuffer} = DBState, position = Pos, last_utc = LastUTC} = Iterator) ->
  <<_:Pos/binary, Buffer/binary>> = FullBuffer,
  {Event, ReadBytes, NewDBState} = case Buffer of
    <<>> -> {eof, 0, DBState};
    _Other -> get_first_packet(Buffer, DBState)
  end,
  case packet_timestamp(Event) of
    Before when LastUTC == undefined orelse Before == undefined orelse Before =< LastUTC ->
      % Event is before given limit or we cannot compare
      {Event, Iterator#iterator{dbstate = NewDBState, position = Pos + ReadBytes}};
    _After ->
      {eof, Iterator}
  end;

%% @doc read from filter: first, try to read from buffer
read_event(#filter{buffer = [Event|BufTail]} = Filter) ->
  {Event, Filter#filter{buffer = BufTail}};

%% @doc read from filter: empty buffer -> pass event from source and retry
read_event(#filter{buffer = [], source = Source, ffun = FFun, state = State} = Filter) ->
  {SrcEvent, NextSource} = read_event(Source),
  {NewBuffer, NextState} = FFun(SrcEvent, State),

  % Filter isn't meant to pass eof, so append it
  RealBuffer = case SrcEvent of
    eof -> NewBuffer ++ [eof];
    _ -> NewBuffer
  end,
  read_event(Filter#filter{
      buffer = RealBuffer,
      source = NextSource,
      state = NextState}).

%% @doc read all events from buffer until eof
all_events(Iterator) ->
  all_events(Iterator, []).

all_events(Iterator, RevEvents) ->
  case read_event(Iterator) of
    {eof, _} ->
      lists:reverse(RevEvents);
    {Event, NextIterator} ->
      all_events(NextIterator, [Event|RevEvents])
  end.

%% @doc get first event from buffer when State is db state at the beginning of it
get_first_packet(Buffer, #dbstate{depth = Depth, last_md = LastMD, scale = Scale} = State) ->
  {ok, Packet, Size} = stockdb_format:decode_packet(Buffer, Depth, LastMD, Scale),
  NextState = case Packet of
    #md{timestamp = Timestamp} ->
      State#dbstate{last_timestamp = Timestamp, last_md = Packet};
    #trade{timestamp = Timestamp} ->
      State#dbstate{last_timestamp = Timestamp}
  end,
  {Packet, Size, NextState}.

% Foldl: low-memory fold over entries
foldl(Fun, Acc0, Iterator) ->
  do_foldl(Fun, Acc0, Iterator).

% foldl_range: fold over entries in specified time range
foldl(Fun, Acc0, Iterator, {_Start, _End} = Range) ->
  do_foldl(Fun, Acc0, set_range(Range, Iterator)).

do_foldl(Fun, AccIn, Iterator) ->
  {Event, NextIterator} = read_event(Iterator),
  case Event of
    eof ->
      % Finish folding -- no more events
      AccIn;
    _event ->
      % Iterate
      AccOut = Fun(Event, AccIn),
      do_foldl(Fun, AccOut, NextIterator)
  end.


packet_timestamp({md, Timestamp, _Bid, _Ask}) -> Timestamp;
packet_timestamp({trade, Timestamp, _Price, _Volume}) -> Timestamp;
packet_timestamp(eof) -> undefined.
