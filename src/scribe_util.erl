-module(scribe_util).
-export([to_lower/1, to_map/1, to_map/2, to_ejson/1, to_ejson/2]).
-export([term_parser/1]).

to_lower(BitString) ->
  String = bitstring_to_list(BitString),
  list_to_bitstring(string:to_lower(String)).

to_map(O) -> to_map(O, fun term_parser/1).

to_map([], _TermParser) -> [];
to_map([H|T], TermParser) ->
  Fn = fun({K,V}, Map) ->
    case is_list(V) of
      false -> maps:put(K,TermParser(V), Map);
      true -> maps:put(K, to_map(V), Map)
    end
  end,
  case H of
    {_,_} -> lists:foldl(Fn, #{}, [H|T]);
    [{_,_}|_] -> lists:map(fun to_map/1, [H|T]);
    _ -> [H|T]
  end.


to_ejson(O) -> to_ejson(O, fun term_parser/1).

to_ejson([], _TermParser) -> [];
to_ejson([H|T], TermParser) ->
  Fn = fun({K,V}) ->
    case is_list(V) of
      false -> {K, TermParser(V)};
      true -> {K,to_ejson(V)}
    end
  end,
  case H of
    {_,_} -> {lists:map(Fn, [H|T])};
    [{_,_}|_] -> lists:map(fun to_ejson/1, [H|T]);
    _ -> [H|T]
  end.


term_parser({{Y,M,D},{H,N,S}}) ->
  Date = string:join([integer_to_list(Y),integer_to_list(M),integer_to_list(D)], "-"),
  Time = string:join([integer_to_list(H),integer_to_list(N),integer_to_list(S)], ":"),
  string:join([Date,Time], "T");

term_parser(Term) -> Term.
