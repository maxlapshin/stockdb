Stockdb
=======


This library is a storage for Stock Exchange quotes.

It is an append-only online-compress storage, that supports failover, indexing and simple lookups of stored quotes.

It can compress 400 000 daily quotes into 4 MB of disk storage.

Usage
=====

First install and compile it. Include it as a rebar dependency.

It can be used either as an appender, either as reader. You cannot mix these two modes now.

Appender
------

    {ok, Appender} = stockdb:open_appender('NASDAQ.AAPL', "2012-01-15", [{depth, 2}]),
    {ok, Appender1} = stockdb:append({md, 1326601810453, [{450.1,100},{449.56,1000}], [{452.43,20},{454.15,40}]}),
    stockdb:close(Appender1).


Now lets explain, what is happening.

* Open appender. Stock name should be a symbol, date should either erlang date {YYYY,MM,DD}, either a string "YYYY-MM-DD".
* Don't forget to specify proper depth. If you skip it, default depth is 1 and you will save only best bid and best ask
* Specify also {scale, 1000} option, if you want to store quotes with precision less than 1 cent. Stockdb stores your prices as int: round(Price*Scale)
* Now append market data.
* Market data is following: ```{md, UtcMilliseconds, [{L1BidPrice,L1BidSize},{L2BidPrice,L2BidSize}..], [{L1AskPrice,L2AskSize}..]}```
* You can include ```-include_lib("stockdb/include/stockdb.hrl").``` to use ```#md{}``` and ```#trade{}``` records

Now take a look at db/stock folder. There you can see new file db/stock/NASDAQ.AAPL-2012-01-15.stock and now you can read back stocks from it.


Reader
-----

The most simple way is just to read all daily events to replaying them

    {ok, Events} = stockdb:events('NASDAQ.AAPL', "2012-01-15").

But there are possible more enhanced ways of limiting amount of loaded data. Will be described soon.