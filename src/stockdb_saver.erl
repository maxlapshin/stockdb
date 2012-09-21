-module(stockdb_saver).

-include("stockdb.hrl").
-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").


-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, 
  terminate/2, code_change/3]).

-export([identity/2]).
-export([add_handler/3, add_sup_handler/3]).

-record(saver, {
  db,
  stock,
  date,
  transform
}).


add_handler(Handler, Stock, Options) ->
  add_specific_handler(Handler, Stock, Options, add_handler).

add_sup_handler(Handler, Stock, Options) ->
  add_specific_handler(Handler, Stock, Options, add_sup_handler).



add_specific_handler(Handler, Stock, Options, Fun) ->
  Module = case proplists:get_value(id, Options) of
    undefined -> ?MODULE;
    Id -> {?MODULE, Id}
  end,
  case lists:member(Module, gen_event:which_handlers(Handler)) of
    true -> ok;
    false -> ok = gen_event:Fun(Handler, Module, [Stock|Options])
  end.



identity(Event, _) -> Event.

init([Stock|Options]) ->
  {Date, _} = calendar:universal_time(),
  {ok,DB} = stockdb:open_append(Stock, Date, Options),
  Transform = proplists:get_value(transform, Options, {?MODULE, identity, []}),
  {ok, #saver{db = DB, stock = Stock, date = Date, transform = Transform}}.


handle_event(RawEvent, #saver{transform = {M,F,A}, db = DB1} = Saver) ->
  Events = case M:F(RawEvent, A) of
    undefined -> [];
    List when is_list(List) -> List;
    Evt when is_tuple(Evt) -> [Evt]
  end,
  DB2 = lists:foldl(fun(Event, DB) ->
    {ok, DB_} = stockdb:append(Event, DB),
    DB_
  end, DB1, Events),
  {ok, Saver#saver{db = DB2}}.


handle_info(_Msg, #saver{} = Saver) ->
  {ok, Saver}.


handle_call(Call, Saver) ->
  {ok, {unknown_call, Call}, Saver}.


terminate(_,#saver{db = DB} = _Saver) -> 
  stockdb:close(DB).

code_change(_,Saver,_) -> {ok,Saver}.
