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
-export([list/1, list/2, list/3,
  read/2, read/3,
  write/3, delete/2,
  push/3, pop/0,
  cache_size/0,
  register_path/3, get_path/2, get_paths/0]).

-record(state, {keys= #{}, auth, cache=[]}).

start_link() -> start_link([]).

start_link(Args) ->
  gen_server:start_link({local,?MODULE}, ?MODULE, Args, []).


% Only for buckets
list(Bucket) ->
  gen_server:call(?MODULE, {list, Bucket}).

% scribe:list(<<"/friends">>, #{<<"$user_id">> => <<"65847190">>}).
list(Key, Map) -> list(Key, Map, false).

list(Key, Map, Meta) ->
  gen_server:call(?MODULE, {list, Key, Map, Meta}).
  

read(Key, Map) -> read(Key, Map, false).

read(Key, Map, Meta) ->
  gen_server:call(?MODULE, {read, Key, Map, Meta}, infinity).

write(Key, Data, Map) ->
  gen_server:cast(?MODULE, {write, Key, Data, Map}).

%% Push a record to the cache. Do this when a write process fails
push(Bucket, Key, Data) ->
  gen_server:cast(?MODULE, {push, Bucket, Key, Data}).

%% Pop a record from the cache and write. Do this after a successful write
pop() ->
  gen_server:cast(?MODULE, pop).

cache_size() ->
  gen_server:call(?MODULE, cache_size).

delete(Key, Map) ->
  gen_server:cast(?MODULE, {delete, Key, Map}).

register_path(Key, Bucket, S3Key) ->
  gen_server:cast(?MODULE, {register_path, Key, {Bucket, S3Key}}).

get_path(Key, Map) ->
  gen_server:call(?MODULE, {get_path, Key, Map}).

get_paths() ->
  gen_server:call(?MODULE, get_paths).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
p_get(K,Map,Default) ->
  try maps:get(K,Map)
  catch _:_ -> Default
  end.

p_get_path(Key, Keys) ->
  %Keys#{Key}.
  p_get(Key,Keys, not_found).

p_get_path(Key, Keys, Map) ->
  %lager:info("[~p] Getting key ~p from ~p", [?MODULE,Key, maps:keys(Keys)]),
  % TODO: Handle non-existent paths
  case p_get(Key,Keys, not_found) of
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
  
p_get_content_type(Data) ->
  case is_bitstring(Data) of
    true -> {Data,"text/plain"};
    false -> {term_to_binary(Data),"application/octet_stream"}
  end.

p_do_write(Bucket, Key, Data, ContentType) ->
  lager:debug("[~p] Writing data to {~p, ~p}", [?MODULE, Bucket,Key]),
  % Spawn this so it doesn't block scribe
  Fn = fun() ->
    try
      HttpHeaders = [{"content-type",ContentType}],
      erlcloud_s3:put_object(Bucket, Key, Data, [], HttpHeaders),
      % If successful, queue up another put
      scribe:pop()
    catch error:{aws_error,{socket_error,E}} ->
      % Cache in state until error resolved (hopefully memory doesn't blow up)
      lager:warning("[~p] Caching due to AWS socket error: ~p", [?MODULE,E]),
      scribe:push(Bucket, Key, Data)
    end
  end,
  erlang:spawn(Fn).

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
  {BinData,ContentType} = p_get_content_type(Data),
  p_do_write(Bucket,Key,BinData, ContentType),
  lager:debug("[~p] Exit handle_cast({write, Bucket, Key, Data})", [?MODULE]),
  {noreply, State};

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


%% Push a record to the cache for a later write
handle_cast({push, Bucket, Key, Data}, #state{cache=Cache}=State) ->
  {noreply, State#state{cache=[{Bucket,Key,Data}|Cache]}};
  
handle_cast(pop, #state{cache=Cache}=State) ->
  State1 = case Cache of
    [] -> State;
    [{B,K,D}|Tail] ->
      lager:debug("[~p] Popping ~p from cache", [?MODULE,K]),
      {Data,ContentType} = p_get_content_type(D),
      p_do_write(B,K,Data,ContentType),
      State#state{cache=Tail}
  end,
  {noreply, State1};

handle_cast({register_path, Key, Path}, #state{keys=Keys}=State) ->
  %NextKeys = Keys#{Key => Path},
  NextKeys = maps:put(Key,Path, Keys),
  lager:info("[~p] Adding ~p => ~p", [?MODULE,Key,Path]),
  {noreply, State#state{keys=NextKeys}}.


handle_call(cache_size, _From, State) ->
  {reply, length(State#state.cache), State};

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

handle_call({list, Key, Map, Meta}, _From, State) ->
  lager:debug("[~p] Enter handle_call({list, Key, Map})", [?MODULE]),
  Content = case p_get_path(Key, State#state.keys, Map) of
    not_found -> not_found;
    {Bucket, K} ->
      lager:debug("[~p] Listing objects in {~p, ~p}", [?MODULE, Bucket,K]),
      try
        PList = erlcloud_s3:list_objects(Bucket, [{prefix,K}] ),
        case Meta of
          true -> PList;
          false ->
            Objects = proplists:get_value(contents,PList),
            [ proplists:get_value(key, Object) || Object <- Objects ]
        end
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

handle_call({read, Key, Map, Meta}, _From, State) ->
  lager:debug("[~p] Enter handle_call({read, Key, Map})", [?MODULE]),
  Content = case p_get_path(Key, State#state.keys, Map) of
    not_found -> not_found;
    {Bucket, K} ->
      lager:debug("[~p] Reading data from {~p, ~p}", [?MODULE, Bucket,K]),
      try
        PList = erlcloud_s3:get_object(Bucket, K),
        case Meta of
          true -> PList;
          false -> proplists:get_value(content,PList)
        end
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
