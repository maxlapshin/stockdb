
-record(md, {
  timestamp :: stockdb:timestamp(),
  bid :: [stockdb:quote()],
  ask :: [stockdb:quote()]
}).

-record(trade, {
  timestamp :: stockdb:timestamp(),
  price :: stockdb:price(),
  volume :: stockdb:volume()
}).


-type open_option() :: {depth, non_neg_integer()} | {scale, non_neg_integer()} | {chunk_size, non_neg_integer()} |
                       {date, term()} | {stock, stockdb:stock()}.

-type filter() :: candle | average.
-type reader_option() :: {filter, filter()} | {range, stockdb:timestamp(), stockdb:timestamp()}.


-type iterator() :: term().
