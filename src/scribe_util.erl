-module(scribe_util).
-export([is_string/1, to_lower/1,
  to_map/1, to_map/2, to_ejson/1, to_ejson/2]).
-export([term_parser/1]).

is_string(List) ->
  Ascii = io_lib:printable_list(List),
  Unicode = io_lib:printable_unicode_list(List),
  Ascii or Unicode.
  
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
    _ -> TermParser([H|T])
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
    _ -> TermParser([H|T])
  end.


term_parser({{Y,M,D},{H,N,S}}) ->
  F4 = fun integer_to_list/1,
  F = fun(X) ->
    string:right(integer_to_list(X),2,$0)
  end,
  Date = list_to_binary(string:join([F4(Y),F(M),F(D)], "-")),
  Time = list_to_binary(string:join([F(H),F(N),F(S)], ":")),
  <<Date/binary,"T",Time/binary>>;

term_parser(Term) when is_list(Term) ->
  case is_string(Term) of
    true -> list_to_binary(Term);
    false -> Term
  end;

term_parser(Term) -> Term.
