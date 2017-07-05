% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
%
% You may obtain a copy of the License at
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
% either express or implied.
%
% See the License for the specific language governing permissions
% and limitations under the License.
%
% This file drew much inspiration from erlview, which was written by and
% copyright Michael McDaniel [http://autosys.us], and is also under APL 2.0
%
%
% This module provides the smallest possible luerl view-server.
% With this module in-place, you can add the following to your couch INI files:
%  [luerl_query_servers]
%  erlang={couch_luerl_process, start_link, []}
%
% Which will then allow following example map function to be used:
%
%  fun({Doc}) ->
%    % Below, we emit a single record - the _id as key, null as value
%    DocId = couch_util:get_value(<<"_id">>, Doc, null),
%    Emit(DocId, null)
%  end.
%
% which should be roughly the same as the javascript:
%    emit(doc._id, null);
%
% This module exposes enough functions such that a luerl erlang server can
% act as a fully-fleged view server, but no 'helper' functions specifically
% for simplifying your erlang view code.  It is expected other third-party
% extensions will evolve which offer useful layers on top of this view server
% to help simplify your view code.
-module(couch_luerl_process).
-behaviour(gen_server).
-vsn(1).

-export([start_link/0,init/1,terminate/2,handle_call/3,handle_cast/2,code_change/3,
         handle_info/2]).
-export([set_timeout/2, prompt/2]).

-define(STATE, luerl_proc_state).
-record(evstate, {ddocs, funs=[], lua_state, query_config=[], list_pid=nil, timeout=5000}).

-include_lib("couch/include/couch_db.hrl").

start_link() ->
    gen_server:start_link(?MODULE, [], []).

% this is a bit messy, see also couch_query_servers handle_info
% stop(_Pid) ->
%     ok.

set_timeout(Pid, TimeOut) ->
    gen_server:call(Pid, {set_timeout, TimeOut}).

prompt(Pid, Data) when is_list(Data) ->
    gen_server:call(Pid, {prompt, Data}).

% gen_server callbacks
init([]) ->
    {ok, #evstate{ddocs=dict:new()}}.

handle_call({set_timeout, TimeOut}, _From, State) ->
    {reply, ok, State#evstate{timeout=TimeOut}};

handle_call({prompt, Data}, _From, State) ->
    couch_log:debug("Prompt luerl qs: ~s",[?JSON_ENCODE(Data)]),
    {NewState, Resp} = try run(State, to_binary(Data)) of
        {S, R} -> {S, R}
        catch
            throw:{error, Why} ->
                {State, [<<"error">>, Why, Why]}
        end,

    case Resp of
        {error, Reason} ->
            Msg = io_lib:format("couch luerl server error: ~p", [Reason]),
            {reply, [<<"error">>, <<"luerl_query_server">>, list_to_binary(Msg)], NewState};
        [<<"error">> | Rest] ->
            % Msg = io_lib:format("couch luerl server error: ~p", [Rest]),
            % TODO: markh? (jan)
            {reply, [<<"error">> | Rest], NewState};
        [<<"fatal">> | Rest] ->
            % Msg = io_lib:format("couch luerl server error: ~p", [Rest]),
            % TODO: markh? (jan)
            {stop, fatal, [<<"error">> | Rest], NewState};
        Resp ->
            {reply, Resp, NewState}
    end.

handle_cast(garbage_collect, State) ->
    erlang:garbage_collect(),
    {noreply, State};
handle_cast(_, State) -> {noreply, State}.

handle_info({'EXIT',_,normal}, State) -> {noreply, State};
handle_info({'EXIT',_,Reason}, State) ->
    {stop, Reason, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVersion, State, _Extra) -> {ok, State}.

run(#evstate{ddocs=DDocs}, [<<"reset">>]) ->
    LuaState = luerl:init(),
    {#evstate{ddocs=DDocs, lua_state=LuaState}, true};
run(#evstate{ddocs=DDocs}, [<<"reset">>, QueryConfig]) ->
    LuaState = luerl:init(),
    {#evstate{ddocs=DDocs, lua_state=luerl:init(), query_config=QueryConfig}, true};
run(#evstate{funs=Chunks, lua_state=LuaState}=State, [<<"add_fun">> , BinFunc]) ->
    {Sig, Chunk, _NewLuaState} = makefun(LuaState, BinFunc),
    {State#evstate{ funs=Chunks++[{Sig,Chunk}] }, true};
run(#evstate{lua_state=LuaState}=State, [<<"map_doc">> , Doc]) ->
    {State, catch mapping(State, Doc)};
run(State, [<<"reduce">>, Funs, KVs]) ->
    {State, catch reduce(State, Funs, KVs, false)};
run(State, [<<"rereduce">>, Funs, Vals]) ->
    {State, catch reduce(State, Funs, null, Vals, true)};
run(_, Unknown) ->
    couch_log:error("Luerl Process: Unknown command: ~p~n", [Unknown]),
    throw({error, unknown_command}).


store_ddoc(DDocs, DDocId, DDoc) ->
    dict:store(DDocId, DDoc, DDocs).
load_ddoc(DDocs, DDocId) ->
    try dict:fetch(DDocId, DDocs) of
        {DDoc} -> {DDoc}
    catch
        _:_Else -> throw({error, ?l2b(io_lib:format("Luerl Query Server missing DDoc with Id: ~s",[DDocId]))})
    end.

bindings(State, Sig) ->
    bindings(State, Sig, nil).
bindings(#evstate{lua_state=LSt0}, Sig, DDoc) ->
    _Self = self(),

    % Elixir example:
    % state1 = :luerl.set_table([:inc], fn ([val], state) -> {[val + 1], state} end, state)

    LSt1 = luerl:set_table([log], fun([Msg], State) ->
        couch_log:info(Msg, [])
    end, LSt0),

    LSt2 = luerl:set_table([emit], fun([Id, Value], _State) ->
        Curr = erlang:get(Sig),
        erlang:put(Sig, [[Id, Value] | Curr])
    end, LSt1),

    LSt2.

% Handle Compilation of Luerl Function
makefun(State, Source) ->
    Sig = couch_crypto:hash(md5, Source),
    LuaStateBound = bindings(State, Sig),
    {Chunk, LuaState} = compilefun(LuaStateBound, Source),
    {Sig, Chunk, LuaStateBound}.
makefun(State, Source, {DDoc}) ->
    Sig = couch_crypto:hash(md5, lists:flatten([Source, term_to_binary(DDoc)])),
    LuaStateBound = bindings(State, Sig, {DDoc}),
    {Chunk, _LuaState} = compilefun(LuaStateBound, Source),
    {Sig, Chunk, LuaStateBound}.
compilefun(LuaState, Source) ->
    % Compile Luerl Function into Chunks / Forms for Luerl VM
    % alternate:
    %     {[func1], st3} = :luerl.do("return function(a,b) return a+b end", st2)
    case luerl:load(Source, LuaState) of
        {ok, Chunk, NewLuaState} ->
          {Chunk, NewLuaState};
        {error, Reason}=Error ->
            couch_log:error("Syntax error on line: ~p~n",
                            Reason),
            throw(Error)
    end.

% Handle performing map/reduce requests
mapping(#evstate{funs=MapFuns, lua_state=LuaState}, Doc) ->
  Resp = lists:map(fun({Sig, Chunk}) ->
      erlang:put(Sig, []),
      % Execute Lua Chunk (akak Form / Function )
      luerl:call_chunk(Chunk, Doc, LuaState),
      % reverse results to match input order
      lists:reverse(erlang:get(Sig))
  end, MapFuns).

reduce(State, BinFuns, Keys, Vals, ReReduce) when is_list(BinFuns) ->
    % Compile Reduce Funs (note: consider caching? )
    ReduceFuns = lists:map(fun(RF) ->
      compilefun(State, RF)
    end, BinFuns),
    % Apply reductions
    Reds = lists:map(fun({Chunk, LuaState}) ->
        luerl:call_chunk(Chunk, [Keys, Vals, ReReduce], LuaState)
    end, ReduceFuns),
    [true, Reds];
reduce(State, BinFun, Keys, Vals, ReReduce) ->
    reduce(State, [BinFun], Keys, Vals, ReReduce).
reduce(State, BinFun, KVs, ReReduce) ->
    {Keys, Vals} = lists:foldl(fun([K, V], {KAcc, VAcc}) ->
        {[K | KAcc], [V | VAcc]}
    end, {[], []}, KVs),
    Keys2 = lists:reverse(Keys),
    Vals2 = lists:reverse(Vals),
    reduce(State, [BinFun], Keys, Vals, ReReduce).


% Convert various data forms to appropriate binary form
to_binary({Data}) ->
    Pred = fun({Key, Value}) ->
        {to_binary(Key), to_binary(Value)}
    end,
    {lists:map(Pred, Data)};
to_binary(Data) when is_list(Data) ->
    [to_binary(D) || D <- Data];
to_binary(null) ->
    null;
to_binary(true) ->
    true;
to_binary(false) ->
    false;
to_binary(Data) when is_atom(Data) ->
    list_to_binary(atom_to_list(Data));
to_binary(Data) ->
    Data.
