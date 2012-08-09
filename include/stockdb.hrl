-type stockdb() :: {stockdb_pid, pid()} | term().

-type price() :: float().
-type volume() :: non_neg_integer().
-type quotes() :: [{price(), volume()}].
-type timestamp() :: non_neg_integer().
-type stock() :: atom().



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


-type open_options() :: append | read | raw.
