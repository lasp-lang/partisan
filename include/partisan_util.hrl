-define(RECORD_TO_LIST(Record),
    lists:zip(?MODULE:record_info(fields, Record), tl(tuple_to_list(Record)))
).

-define(RECORD_TO_MAP(Record),
    maps:from_list(?RECORD_TO_LIST(Record))
).


-define(IS_IP(X), (?IS_IP4(X) orelse ?IS_IP6(X))).

%% copied from inet_int.hrl
%% macro for guards only that checks IP address {A,B,C,D}
%% that returns true for an IP address, but returns false
%% or crashes for other terms
-define(IS_IP4(A, B, C, D),
    (((A) bor (B) bor (C) bor (D)) band (bnot 16#ff)) =:= 0
).

%% d:o for IP address as one term
-define(IS_IP4(Addr),
    (tuple_size(Addr) =:= 4 andalso
     ?IS_IP4(element(1, (Addr)), element(2, (Addr)),
         element(3, (Addr)), element(4, (Addr))))
).

%% d:o IPv6 address
-define(IS_IP6(A, B, C, D, E, F, G, H),
    (((A) bor (B) bor (C) bor (D) bor (E) bor (F) bor (G) bor (H))
     band (bnot 16#ffff)) =:= 0).

-define(IS_IP6(Addr),
    (tuple_size(Addr) =:= 8 andalso
     ?IS_IP6(element(1, (Addr)), element(2, (Addr)),
          element(3, (Addr)), element(4, (Addr)),
          element(5, (Addr)), element(6, (Addr)),
          element(7, (Addr)), element(8, (Addr))))
).


-define(IS_PORT_NBR(N), (N >= 1 andalso N =< 65535)).