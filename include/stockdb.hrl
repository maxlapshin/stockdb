-type stockdb() :: {stockdb_pid, pid()} | term().

-type price() :: float().
-type volume() :: non_neg_integer().
-type quotes() :: [{price(), volume()}].
-type timestamp() :: non_neg_integer().
-type stock() :: atom().
-type date() :: string().


-record(md, {
  timestamp :: timestamp(),
  bid :: quotes(),
  ask :: quotes()
}).

-record(trade, {
  timestamp :: timestamp(),
  price :: price(),
  volume :: volume()
}).

-type market_data() :: #md{}.
-type trade() :: #trade{}.

-type filter() :: candle | average.
-type reader_option() :: {filter, filter()} | {range, timestamp(), timestamp()}.

-type iterator() :: term().
