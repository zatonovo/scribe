%% Make sure AWS keys are properly read from environment
%% erlcloud_s3:list_objects("panoptez").
%% erlcloud_s3:get_object("panoptez","twitter/").
%%
%% Map = #{<<"$session_id">> => <<"3289289234289">>, <<"$user_id">> => <<"brian">>}.
%% scribe:register_path(<<"/sessions/access_token">>, "zn_legion", <<"$session_id/access_token">>).
%% scribe:register_path(<<"/sessions/user_id">>, "zn_legion", <<"$session_id/user_id">>).
%% scribe:register_path(<<"/user/info">>, "zn_twitter", <<"$user_id/info">>).
%% scribe:register_path(<<"/followers/ids">>, "zn_twitter", <<"$user_id/followers">>).
%% scribe:register_path(<<"/friends/ids">>, "zn_twitter", <<"$user_id/friends">>).
%% scribe:get_path(<<"/sessions">>, Map).
%% scribe:get_path(<<"/followers/ids">>, Map).
-module(scribe).
-behavior(gen_server).
-export([start_link/0, start_link/1,
  handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).
-export([list/1, list/2, read/2, write/3, delete/2,
  register_path/3, get_path/2, get_paths/0]).

-record(state, {keys= #{}, auth, cache=[]}).

start_link() -> start_link([]).

start_link(Args) ->
  gen_server:start_link({local,?MODULE}, ?MODULE, Args, []).


% Only for buckets
list(Bucket) ->
  gen_server:call(?MODULE, {list, Bucket}).

% scribe:list(<<"/friends">>, #{<<"$user_id">> => <<"65847190">>}).
list(Key, Map) ->
  gen_server:call(?MODULE, {list, Key, Map}).
  

read(Key, Map) ->
  gen_server:call(?MODULE, {read, Key, Map}, infinity).

write(Key, Data, Map) ->
  gen_server:cast(?MODULE, {write, Key, Data, Map}).

delete(Key, Map) ->
  gen_server:cast(?MODULE, {delete, Key, Map}).

register_path(Key, Bucket, S3Key) ->
  gen_server:cast(?MODULE, {register_path, Key, {Bucket, S3Key}}).

get_path(Key, Map) ->
  gen_server:call(?MODULE, {get_path, Key, Map}).

get_paths() ->
  gen_server:call(?MODULE, get_paths).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
p_get_path(Key, Keys) ->
  %Keys#{Key}.
  maps:get(Key,Keys, not_found).

p_get_path(Key, Keys, Map) ->
  %lager:info("[~p] Getting key ~p from ~p", [?MODULE,Key, maps:keys(Keys)]),
  % TODO: Handle non-existent paths
  case maps:get(Key,Keys, not_found) of
    not_found -> not_found;
    {Bucket,RawPath} ->
      MKeys = maps:keys(Map),
      %lager:debug("[~p] Using params with keys: ~p", [?MODULE,MKeys]),
      Fn = fun(MK, Path) ->
        lager:debug("[~p] Dereferencing ~p in ~p with ~p", 
          [?MODULE,MK,Path, maps:get(MK,Map)]),
        binary:replace(Path, MK, maps:get(MK,Map))
      end,
      {Bucket, binary_to_list(lists:foldl(Fn, RawPath, MKeys))}
  end.
  
p_do_write(Bucket, Key, Data, ContentType, #state{}=State) ->
  lager:debug("[~p] Writing data to {~p, ~p}", [?MODULE, Bucket,Key]),
  try
    HttpHeaders = [{"content-type",ContentType}],
    % This blocks
    erlcloud_s3:put_object(Bucket, Key, Data, [], HttpHeaders),
    % So if successful, queue up another put
    case State#state.cache of
      [] -> State;
      [{B,K,D}|Tail] ->
        lager:debug("[~p] Popping ~p from cache", [?MODULE,K]),
        scribe:write(B,K,D),
        State#state{cache=Tail}
    end
  catch error:{aws_error,{socket_error,E}} ->
    lager:warning("[~p] Caching due to AWS socket error: ~p", [?MODULE,E]),
    % Cache in state until error resolved (hopefully memory doesn't blow up)
    Cache = State#state.cache,
    State#state{cache=[{Bucket,Key,Data}|Cache]}
  end.

p_do_delete(Bucket, Key) ->
  try
    lager:info("[~p] Attempting to delete {~p,~p}", [?MODULE,Bucket,Key]),
    erlcloud_s3:delete_object(Bucket,Key)
  catch error:{aws_error,{socket_error,E}} ->
    % TODO: Need a caching mechanism to remove these as well, although 
    % for sessions, they will eventually get cleaned up
    Msg = "[~p] Unable to delete {~p,~p} due to AWS socket error: ~p",
    lager:warning(Msg, [?MODULE,Bucket,Key,E])
  end.

p_deserialize(Content) ->
  case Content of
    not_found -> not_found;
    {error, E} -> E;
    _ -> try binary_to_term(Content)
      catch error:badarg -> Content
      end
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Args) ->
  lager:info("[~p] Starting up",[?MODULE]),
  {ok, #state{}}.

handle_cast({write, Key, Data, Map}, State) when is_map(Map) ->
  lager:debug("[~p] Enter handle_cast({write, Key, Data, Map})", [?MODULE]),
  case p_get_path(Key, State#state.keys, Map) of
    not_found -> 
      lager:warning("[~p] No path found for key ~p", [?MODULE,Key]),
      {noreply, State};
    {Bucket, K} ->
      lager:debug("[~p] Exit handle_cast({write, Key, Data, Map})", [?MODULE]),
      handle_cast({write, Bucket, K, Data}, State)
  end;

handle_cast({write, Bucket, Key, Data}, State) ->
  lager:debug("[~p] Enter handle_cast({write, Bucket, Key, Data})", [?MODULE]),
  {BinData,ContentType} = case is_bitstring(Data) of
    true -> {Data,"text/plain"};
    false -> {term_to_binary(Data),"application/octet_stream"}
  end,
  NextState = p_do_write(Bucket,Key,BinData, ContentType, State),
  lager:debug("[~p] Exit handle_cast({write, Bucket, Key, Data})", [?MODULE]),
  {noreply, NextState};

handle_cast({delete, Key, Map}, State) ->
  lager:debug("[~p] Enter handle_cast({delete, Key, Map})", [?MODULE]),
  case p_get_path(Key, State#state.keys, Map) of
    not_found -> 
      lager:warning("[~p] No path found for key ~p", [?MODULE,Key]);
    {Bucket, K} ->
      p_do_delete(Bucket, K)
  end,
  lager:debug("[~p] Exit handle_cast({write, Key, Data, Map})", [?MODULE]),
  {noreply, State};

handle_cast({register_path, Key, Path}, #state{keys=Keys}=State) ->
  %NextKeys = Keys#{Key => Path},
  NextKeys = maps:put(Key,Path, Keys),
  lager:info("[~p] Adding ~p => ~p", [?MODULE,Key,Path]),
  {noreply, State#state{keys=NextKeys}}.


handle_call(get_paths, _From, State) ->
  {reply, State#state.keys, State};

handle_call({get_path, Key, Map}, _From, State) ->
  Reply = p_get_path(Key, State#state.keys, Map),
  {reply, Reply, State};

handle_call({list, Bucket}, _From, State) ->
  lager:debug("[~p] Enter handle_call({list, Bucket})", [?MODULE]),
  Reply = erlcloud_s3:list_objects(Bucket),
  lager:debug("[~p] Exit handle_call({list, Bucket})", [?MODULE]),
  {reply, Reply, State};

handle_call({list, Key, Map}, _From, State) ->
  lager:debug("[~p] Enter handle_call({list, Key, Map})", [?MODULE]),
  Content = case p_get_path(Key, State#state.keys, Map) of
    not_found -> not_found;
    {Bucket, K} ->
      lager:debug("[~p] Listing objects in {~p, ~p}", [?MODULE, Bucket,K]),
      try
        PList = erlcloud_s3:list_objects(Bucket, [{prefix,K}] ),
        Objects = proplists:get_value(contents,PList),
        [ proplists:get_value(key, Object) || Object <- Objects ]
      catch 
        error:{aws_error, {http_error, 404,_,_}} -> not_found;
        error:Error ->
          Msg = "[~p] Unable to read {~p,~p}: ~p",
          lager:warning(Msg, [?MODULE,Bucket,K,Error]),
          {error,Error}
      end
  end,
  Reply = p_deserialize(Content),
  lager:debug("[~p] Exit handle_call({list, Key, Map})", [?MODULE]),
  {reply, Reply, State};

handle_call({read, Key, Map}, _From, State) ->
  lager:debug("[~p] Enter handle_call({read, Key, Map})", [?MODULE]),
  Content = case p_get_path(Key, State#state.keys, Map) of
    not_found -> not_found;
    {Bucket, K} ->
      lager:debug("[~p] Reading data from {~p, ~p}", [?MODULE, Bucket,K]),
      try
        PList = erlcloud_s3:get_object(Bucket, K),
        proplists:get_value(content,PList)
      catch 
        error:{aws_error, {http_error, 404,_,_}} -> not_found;
        error:Error ->
          Msg = "[~p] Unable to read {~p,~p}: ~p",
          lager:warning(Msg, [?MODULE,Bucket,K,Error]),
          {error,Error}
      end
  end,
  Reply = p_deserialize(Content),
  lager:debug("[~p] Exit handle_call({read, Key, Map})", [?MODULE]),
  {reply, Reply, State}.


handle_info(_, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Args, _State) ->
  ok.
