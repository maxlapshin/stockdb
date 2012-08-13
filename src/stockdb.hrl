
-record(dbstate, {
    version,
    sync = true,
    file,
    path,
    mode :: read|append,
    buffer,
    buffer_end,
    stock,
    date,
    depth,
    scale,
    chunk_size,
    last_md,
    last_timestamp = 0,
    last_bidask,
    next_chunk_time = 0,
    chunk_map_offset,
    chunk_map = [],
    daystart
  }).


-define(STOCKDB_VERSION, 2).


-define(STOCKDB_OPTIONS, [
    {version, ?STOCKDB_VERSION},
    {stock, undefined},
    {date, utcdate()},
    {depth, 10},
    {scale, 100},
    {chunk_size, 300} % seconds
  ]).

-define(OFFSETLEN, 32).

-define(NUMBER_OF_CHUNKS(ChunkSize),
  timer:hours(24) div timer:seconds(ChunkSize) + 1).
