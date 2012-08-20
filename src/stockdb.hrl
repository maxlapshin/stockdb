
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


-define(assertEqualEps(Expect, Expr, Eps),
	((fun (__X) ->
	    case abs(Expr - Expect) of
		__Y when __Y < Eps -> ok;
		__V -> .erlang:error({assertEqualEps_failed,
				      [{module, ?MODULE},
				       {line, ?LINE},
				       {expression, (??Expr)},
				       {epsilon, io_lib:format("~.5f", [Eps*1.0])},
				       {expected, io_lib:format("~.4f", [Expect*1.0])},
				       {value, io_lib:format("~.4f", [Expr*1.0])}]})
	    end
	  end)(Expect))).

-define(_assertEqualEps(Expect, Expr), ?_test(?assertEqualEps(Expect, Expr))).

-define(assertEqualMD(MD1, MD2), 
  stockdb_test_helper:assertEqualMD(MD1, MD2, [{module,?MODULE},{line,?LINE},{md1,(??MD1)},{md2,(??MD2)}])).
