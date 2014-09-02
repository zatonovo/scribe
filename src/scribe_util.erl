-module(scribe_util).
-export([to_lower/1]).

to_lower(BitString) ->
  String = bitstring_to_list(BitString),
  list_to_bitstring(string:to_lower(String)).
