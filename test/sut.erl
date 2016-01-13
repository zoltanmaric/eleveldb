-module(sut).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%%=======================================================================
%% This is all stuff we need to mock up msgpack-encoding of riak objects
%%=======================================================================

-define(V1_VERS, 1).
-define(MAGIC, 53).
-define(EMPTY_VTAG_BIN, <<"e">>).
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(MD_DELETED,  <<"X-Riak-Deleted">>).
-define(MD_CTYPE,    <<"content-type">>).
-define(MAX_KEY_SIZE, 65536).
-define(DAYS_FROM_GREGORIAN_BASE_TO_EPOCH, (1970*365+478)).
-define(SECONDS_FROM_GREGORIAN_BASE_TO_EPOCH,
        (?DAYS_FROM_GREGORIAN_BASE_TO_EPOCH * 24*60*60)
       ).

-export_type([vclock/0, timestamp/0, vclock_node/0, dot/0, pure_dot/0]).
-export_type([riak_object/0, bucket/0, key/0, value/0, binary_version/0]).

-type vclock() :: [dot()].
-type dot() :: {vclock_node(), {counter(), timestamp()}}.
-type pure_dot() :: {vclock_node(), counter()}.

% Nodes can have any term() as a name, but they must differ from each other.

-type   vclock_node() :: term().
-type   counter() :: integer().
-type   timestamp() :: integer().

% @doc Create a brand new vclock.
-spec fresh() -> vclock().
fresh() ->
    [].

-spec fresh(vclock_node(), counter()) -> vclock().
fresh(Node, Count) ->
    [{Node, {Count, timestamp()}}].

% @doc Return a timestamp for a vector clock
-spec timestamp() -> timestamp().
timestamp() ->
    %% Same as calendar:datetime_to_gregorian_seconds(erlang:universaltime()),
    %% but significantly faster.
    {MegaSeconds, Seconds, _} = os:timestamp(),
        ?SECONDS_FROM_GREGORIAN_BASE_TO_EPOCH + MegaSeconds*1000000 + Seconds.

-type key() :: binary().
-type bucket() :: binary() | {binary(), binary()}.
-type value() :: term().

-record(r_content, {
          metadata :: dict() | list(),
          value :: term()
         }).

-record(r_object, {
          bucket :: bucket(),
          key :: key(),
          contents :: [#r_content{}],
          vclock = fresh() :: vclock(),
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).

-opaque riak_object() :: #r_object{}.

%% @doc Constructor for new riak objects.
-spec new(Bucket::bucket(), Key::key(), Value::value()) -> riak_object().
new({T, B}, K, V) when is_binary(T), is_binary(B), is_binary(K) ->
    new_int({T, B}, K, V, no_initial_metadata);
new(B, K, V) when is_binary(B), is_binary(K) ->
    new_int(B, K, V, no_initial_metadata).

%% @doc Constructor for new riak objects with an initial content-type.
-spec new(Bucket::bucket(), Key::key(), Value::value(),
          string() | dict() | no_initial_metadata) -> riak_object().
new({T, B}, K, V, C) when is_binary(T), is_binary(B), is_binary(K), is_list(C) ->
    new_int({T, B}, K, V, dict:from_list([{?MD_CTYPE, C}]));
new(B, K, V, C) when is_binary(B), is_binary(K), is_list(C) ->
    new_int(B, K, V, dict:from_list([{?MD_CTYPE, C}]));

%% @doc Constructor for new riak objects with an initial metadata dict.
%%
%% NOTE: Removed "is_tuple(MD)" guard to make Dialyzer happy.  The previous clause
%%       has a guard for string(), so this clause is OK without the guard.
new({T, B}, K, V, MD) when is_binary(T), is_binary(B), is_binary(K) ->
    new_int({T, B}, K, V, MD);
new(B, K, V, MD) when is_binary(B), is_binary(K) ->
    new_int(B, K, V, MD).

%% internal version after all validation has been done
new_int(B, K, V, MD) ->
    case size(K) > ?MAX_KEY_SIZE of
        true ->
            throw({error,key_too_large});
        false ->
            case MD of
                no_initial_metadata ->
                    Contents = [#r_content{metadata=dict:new(), value=V}],
                    #r_object{bucket=B,key=K,
                              contents=Contents,vclock=fresh()};
                _ ->
                    Contents = [#r_content{metadata=MD, value=V}],
                    #r_object{bucket=B,key=K,updatemetadata=MD,
                              contents=Contents,vclock=fresh()}
            end
    end.

is_robject(#r_object{}) ->
    true;
is_robject(_) ->
    false.

-spec get_contents(riak_object()) -> [{dict(), term()}].
get_contents(#r_object{contents=Contents}) ->
    [{Content#r_content.metadata, Content#r_content.value} ||
        Content <- Contents].

-spec bucket(riak_object()) -> bucket().
bucket(Obj1) ->
    Obj1#r_object.bucket.

-spec contents(riak_object()) -> term().
contents(Obj1) ->
    Obj1#r_object.contents#r_content.value.


-type binary_version() :: v0 | v1.
-type encoding() :: erlang | msgpack | ei | pb.


-spec to_binary(binary_version(), riak_object()) -> binary().
-spec to_binary(binary_version(), riak_object(), encoding()) -> binary().

to_binary(Vers, Robj) ->
    to_binary(Vers, Robj, erlang).
to_binary(v0, RObj, _) ->
    term_to_binary(RObj);
to_binary(v1, #r_object{contents=Contents, vclock=VClock}, Enc) ->
    new_v1(VClock, Contents, Enc).

new_v1(Vclock, Siblings, Enc) ->
    VclockBin = term_to_binary(Vclock),
    VclockLen = byte_size(VclockBin),
    SibCount = length(Siblings),
    SibsBin = bin_contents(Siblings, Enc),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer, VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.

bin_content(#r_content{metadata=Meta, value=Val}, Enc) ->
    ValBin = encode(Val, Enc),
    ValLen = byte_size(ValBin),
    MetaBin = meta_bin(Meta),
    MetaLen = byte_size(MetaBin),
    <<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary>>.

bin_contents(Contents, Enc) ->
    F = fun(Content, Acc) ->
                <<Acc/binary, (bin_content(Content, Enc))/binary>>
        end,
    lists:foldl(F, <<>>, Contents).

meta_bin(MD) ->
    {{VTagVal, Deleted, LastModVal}, RestBin} = dict:fold(fun fold_meta_to_bin/3,
                                                          {{undefined, <<0>>, undefined}, <<>>},
                                                          MD),
    VTagBin = case VTagVal of
                  undefined ->  ?EMPTY_VTAG_BIN;
                  _ -> list_to_binary(VTagVal)
              end,
    VTagLen = byte_size(VTagBin),
    LastModBin = case LastModVal of
                     undefined -> <<0:32/integer, 0:32/integer, 0:32/integer>>;
                     {Mega,Secs,Micro} -> <<Mega:32/integer, Secs:32/integer, Micro:32/integer>>
                 end,
    <<LastModBin/binary, VTagLen:8/integer, VTagBin:VTagLen/binary,
      Deleted:1/binary-unit:8, RestBin/binary>>.

fold_meta_to_bin(?MD_VTAG, Value, {{_Vt,Del,Lm},RestBin}) ->
    {{Value, Del, Lm}, RestBin};
fold_meta_to_bin(?MD_LASTMOD, Value, {{Vt,Del,_Lm},RestBin}) ->
     {{Vt, Del, Value}, RestBin};
fold_meta_to_bin(?MD_DELETED, true, {{Vt,_Del,Lm},RestBin})->
     {{Vt, <<1>>, Lm}, RestBin};
fold_meta_to_bin(?MD_DELETED, "true", Acc) ->
    fold_meta_to_bin(?MD_DELETED, true, Acc);
fold_meta_to_bin(?MD_DELETED, _, {{Vt,_Del,Lm},RestBin}) ->
    {{Vt, <<0>>, Lm}, RestBin};
fold_meta_to_bin(Key, Value, {{_Vt,_Del,_Lm}=Elems,RestBin}) ->
    ValueBin = encode_maybe_binary(Value),
    ValueLen = byte_size(ValueBin),
    KeyBin = encode_maybe_binary(Key),
    KeyLen = byte_size(KeyBin),
    MetaBin = <<KeyLen:32/integer, KeyBin/binary, ValueLen:32/integer, ValueBin/binary>>,
    {Elems, <<RestBin/binary, MetaBin/binary>>}.

encode(Bin, Enc) when Enc == erlang ->
    encode_maybe_binary(Bin);
encode(Bin, Enc) when Enc == ei ->
    encode_ei(Bin);
encode(Bin, Enc) when Enc == msgpack ->
    encode_msgpack(Bin);
encode(Bin, Enc) when Enc == pb ->
    encode_pb(Bin).

encode_msgpack(Bin) ->
    MsgBin = msgpack:pack(Bin, [{format, jsx}]),
    MsgBin.

encode_ei(Bin) ->
    term_to_binary(Bin).

encode_pb(Bin) ->
    eleveldb:eniftest({pbenc, Bin}).

em(Bin) ->
    MsgBin = msgpack:pack(Bin, [{format, jsx}]),
    MsgBin.

encode_maybe_binary(Bin) when is_binary(Bin) ->
    <<1, Bin/binary>>;
encode_maybe_binary(Bin) ->    
    <<0, (term_to_binary(Bin))/binary>>.


%% @doc return the binary version the riak object binary is encoded in
-spec binary_version(binary()) -> binary_version().
binary_version(<<131,_/binary>>) -> v0;
binary_version(<<?MAGIC:8/integer, 1:8/integer, _/binary>>) -> v1.

%% @doc Convert binary object to riak object
-spec from_binary(bucket(),key(),binary()) ->
    riak_object() | {error, 'bad_object_format'}.
from_binary(B,K,Obj) ->
    from_binary(B,K,Obj,erlang).

-spec from_binary(bucket(),key(),binary(), encoding()) ->
    riak_object() | {error, 'bad_object_format'}.
from_binary(_B,_K,<<131, _Rest/binary>>=ObjTerm, _) ->
    binary_to_term(ObjTerm);
from_binary(B,K,<<?MAGIC:8/integer, 1:8/integer, Rest/binary>>=_ObjBin, Enc) ->
    %% Version 1 of binary riak object
    case Rest of
        <<VclockLen:32/integer, VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>> ->
            Vclock = binary_to_term(VclockBin),
            Contents = sibs_of_binary(SibCount, SibsBin, Enc),
            #r_object{bucket=B,key=K,contents=Contents,vclock=Vclock};
        _Other ->
            {error, bad_object_format}
    end;
from_binary(_B, _K, Obj = #r_object{}, _) ->
    Obj.

sibs_of_binary(Count,SibsBin, Enc) ->
    sibs_of_binary(Count, SibsBin, [], Enc).

sibs_of_binary(0, <<>>, Result, _) -> lists:reverse(Result);
sibs_of_binary(0, _NotEmpty, _Result, _) ->
    {error, corrupt_contents};
sibs_of_binary(Count, SibsBin, Result, Enc) ->
    {Sib, SibsRest} = sib_of_binary(SibsBin, Enc),
    sibs_of_binary(Count-1, SibsRest, [Sib | Result], Enc).

sib_of_binary(<<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary, Rest/binary>>, Enc) ->
    <<LMMega:32/integer, LMSecs:32/integer, LMMicro:32/integer, VTagLen:8/integer, VTag:VTagLen/binary, Deleted:1/binary-unit:8, MetaRestBin/binary>> = MetaBin,

    MDList0 = deleted_meta(Deleted, []),
    MDList1 = last_mod_meta({LMMega, LMSecs, LMMicro}, MDList0),
    MDList2 = vtag_meta(VTag, MDList1),
    MDList = meta_of_binary(MetaRestBin, MDList2),
    MD = dict:from_list(MDList),
    {#r_content{metadata=MD, value=decode(ValBin, Enc)}, Rest}.

deleted_meta(<<1>>, MDList) ->
    [{?MD_DELETED, "true"} | MDList];
deleted_meta(_, MDList) ->
    MDList.

last_mod_meta({0, 0, 0}, MDList) ->
    MDList;
last_mod_meta(LM, MDList) ->
    [{?MD_LASTMOD, LM} | MDList].

vtag_meta(?EMPTY_VTAG_BIN, MDList) ->
    MDList;
vtag_meta(VTag, MDList) ->
    [{?MD_VTAG, binary_to_list(VTag)} | MDList].

meta_of_binary(<<>>, Acc) ->
    Acc;
meta_of_binary(<<KeyLen:32/integer, KeyBin:KeyLen/binary, ValueLen:32/integer, ValueBin:ValueLen/binary, Rest/binary>>, ResultList) ->
    Key = decode_maybe_binary(KeyBin),
    Value = decode_maybe_binary(ValueBin),
    meta_of_binary(Rest, [{Key, Value} | ResultList]).

decode(ValBin, Enc) when Enc == erlang ->
    decode_maybe_binary(ValBin);
decode(ValBin, Enc) when Enc == msgpack ->
    decode_msgpack(ValBin);
decode(ValBin, Enc) when Enc == ei ->
    decode_ei(ValBin);
decode(ValBin, Enc) when Enc == pb ->
    decode_pb(ValBin).

decode_msgpack(ValBin) ->
    {ok, Unpacked} = msgpack:unpack(ValBin, [{format, jsx}]),
    Unpacked.

decode_ei(ValBin) ->
    binary_to_term(ValBin).

decode_pb(ValBin) ->
    eleveldb:eniftest({pbdec, ValBin}).

decode_maybe_binary(<<1, Bin/binary>>) ->
    Bin;
decode_maybe_binary(<<0, Bin/binary>>) ->
    binary_to_term(Bin).

%%=======================================================================
%% Generic utilities we need to run tests
%%=======================================================================

open() ->
    {ok, Ref} = eleveldb:open("ltest", [{create_if_missing, true}]),
    Ref.

open(Table) ->
    {ok, Ref} = eleveldb:open(Table, [{create_if_missing, true}]),
    Ref.

close(Ref) ->
    eleveldb:close(Ref).

clearDb() ->
    os:cmd("rm -rf ltest").

myformat(T) ->
    io:format("\r"),
    io:format(T),
    io:nl().

myformat(T,Val) ->
    io:format("\r"),
    io:format(T,[Val]),
    io:nl().

myformat(T,Val1, Val2) ->
    io:format("\r"),
    io:format(T,[Val1, Val2]),
    io:nl().

%%=======================================================================
%% Utilities needed for streaming fold tests
%%=======================================================================

%%------------------------------------------------------------
%% Recursive function to putkeys in the database
%%------------------------------------------------------------

putKeysObj(N) -> 
    putKeysObj(N, msgpack).

putKeysObj(N, Encoding) -> 
    clearDb(),
    putKeysObj(open(), N, Encoding).
putKeysObj(Ref,N,Encoding) -> 
    putKeysObj(Ref,N,1, Encoding).
putKeysObj(Ref,N,Acc,Encoding) when Acc =< N -> 
    ValList = [{<<"f1">>, Acc},  {<<"f2">>, <<"test2">>}, {<<"f3">>, 3}, {<<"f4">>, [1,2,3]}, {<<"f5">>, false}],
    addKey(Ref, ValList, Acc, N, Encoding);
putKeysObj(Ref,N,Acc,_Encoding) when Acc > N ->
    close(Ref).

%%------------------------------------------------------------
%% Iterative function add a key to the backend
%%------------------------------------------------------------

addKey(Ref, ValList, Acc, N, Encoding) ->
    Ndig = trunc(math:log10(N)) + 1,
    Key  = list_to_binary("key" ++ string:right(integer_to_list(Acc), Ndig, $0)),
    Obj  = new(<<"bucket">>, Key, ValList),
    Val  = to_binary(v1, Obj, Encoding),
    ok   = eleveldb:put(Ref, Key, Val, []),
    putKeysObj(Ref,N,Acc+1, Encoding).

getObj(ValList) ->
    Obj  = new(<<"bucket">>, <<"key">>, ValList),
    Val  = to_binary(v1, Obj, erlang),
    {Obj, Val}.

%%------------------------------------------------------------
%% Explicit function to add a key to the backend
%%------------------------------------------------------------

addKey(Ref, KeyNum, ValList) ->
    addKey(Ref, KeyNum, ValList, msgpack).

addKey(Ref, KeyNum, ValList, Encoding) ->
    Key = list_to_binary("key"++integer_to_list(KeyNum)),
    Obj = new(<<"bucket">>, Key, ValList),
    Val = to_binary(v1, Obj, Encoding),
    ok = eleveldb:put(Ref, Key, Val, []).

%%------------------------------------------------------------
%% Get the value (contents) of a msgpack-encoded riak object
%%------------------------------------------------------------

getKeyVal(K,V) ->
    getKeyVal(K,V,msgpack).

getKeyValEi(K,V) ->
    getKeyVal(K,V,ei).

getKeyValPb(K,V) ->
    getKeyVal(K,V,pb).

getKeyVal(K,V,Encoding) ->
    Obj = from_binary(<<"bucket">>, K, V, Encoding),
    [{_,Contents}] = get_contents(Obj),
    Contents.

getKeyVal(B,K,V,Encoding) ->
    Obj = from_binary(B, K, V, Encoding),
    [{_,Contents}] = get_contents(Obj),
    Contents.

%%------------------------------------------------------------
%% Fold over keys using streaming folds
%%------------------------------------------------------------

streamFoldTest(Filter, PutKeyFun) ->
    clearDb(),
    case Filter of
	{} ->
	    Opts=[{fold_method, streaming},
		  {encoding, msgpack}];
	_ ->
	    Opts=[{fold_method, streaming},
		  {range_filter, Filter},
		  {encoding, msgpack}]
    end,
    io:format("Opts = ~p~n", [lists:flatten(Opts)]),
    Ref = open(),

    PutKeyFun(Ref),

% Build a list of returned keys

    FF = fun({K,V}, Acc) -> 
		 [getKeyVal(K,V) | Acc]
	 end,

    Acc = eleveldb:fold(Ref, FF, [], Opts),
    ok = eleveldb:close(Ref),
    lists:reverse(Acc).

streamFoldTestErlang(Filter, PutKeyFun) ->
    clearDb(),
    case Filter of
	{} ->
	    Opts=[{fold_method, streaming},
		  {encoding, ei},
		  {start_key, <<"key1">>}];
	_ ->
	    Opts=[{fold_method, streaming},
		  {range_filter, Filter},
		  {encoding, ei},
		  {start_key, <<"key1">>}]
    end,
    io:format("Opts = ~p~n", [lists:flatten(Opts)]),
    Ref = open(),

    PutKeyFun(Ref),

% Build a list of returned keys

    FF = fun({K,V}, Acc) -> 
		 [getKeyValEi(K,V) | Acc]
	 end,

    Acc = eleveldb:fold(Ref, FF, [], Opts),
    ok = eleveldb:close(Ref),
    lists:reverse(Acc).

streamFoldTestPb(Filter, PutKeyFun) ->
    clearDb(),
    case Filter of
	{} ->
	    Opts=[{fold_method, streaming},
		  {encoding, pb},
		  {start_key, <<"key1">>}];
	_ ->
	    Opts=[{fold_method, streaming},
		  {range_filter, Filter},
		  {encoding, pb},
		  {start_key, <<"key1">>}]
    end,
    io:format("Opts = ~p~n", [lists:flatten(Opts)]),
    Ref = open(),

    PutKeyFun(Ref),

% Build a list of returned keys

    FF = fun({K,V}, Acc) -> 
		 [getKeyValPb(K,V) | Acc]
	 end,

    Acc = eleveldb:fold(Ref, FF, [], Opts),
    ok = eleveldb:close(Ref),
    lists:reverse(Acc).

get_field(Field, List) ->
    lists:keyfind(Field, 1, List).

match([], _, _, _) ->
    0;
match(List, Field1, Field2, CompFun) when is_binary(Field2) ->
    {_,Val1} = get_field(Field1, List),
    {_,Val2} = get_field(Field2, List),
    Match = match(Val1,Val2,CompFun),
    Match;
match(List, Field, CompVal, CompFun) ->
    {_,Val} = get_field(Field, List),
    Match = match(Val,CompVal,CompFun),
    Match.

match(V1,{CompVal},CompFun) ->
    match(V1, CompVal, CompFun);
match(V1,{_FilterVal, CompVal},CompFun) ->
    match(V1, CompVal, CompFun);
match(V1,V2,CompFun) ->
    case CompFun(V1,V2) of
	true -> 1;
	_ -> 0
    end.

fieldsMatching(Vals, Field, CompVal, CompFun) ->
    lists:foldl(fun(Val, {N, Nmatch}) -> 
			{N + 1, Nmatch + match(Val, Field, CompVal, CompFun)} end, {0,0}, Vals).

%%=======================================================================
%% Actual tests begin here
%%=======================================================================

%%=======================================================================
%% Test that we can pack and unpack erlang/msgpack-encoded objects
%%=======================================================================

packObj_test() ->
    io:format("packObj_test~n"),
    Obj = new(<<"bucket">>, <<"key">>, [{<<"field1">>, 1}, {<<"field2">>, 2.123}]),
    PackedErl = to_binary(v1, Obj, erlang),
    PackedMsg = to_binary(v1, Obj, msgpack),
    ObjErl = from_binary(<<"bucket">>, <<"key">>, PackedErl, erlang),
    ObjMsg = from_binary(<<"bucket">>, <<"key">>, PackedMsg, msgpack),
    Res = (ObjErl == Obj) and (ObjMsg == Obj),
    ?assert(Res),
    Res.

%%=======================================================================
%% Operations tests
%%=======================================================================

%%-----------------------------------------------------------------------
%% Utilities needed for operations tests
%%-----------------------------------------------------------------------

putKeySingleOpsEi(Ref) ->
    putKeyNormalOps(Ref, ei).

putKeySingleOpsMsgpack(Ref) ->
    putKeySingleOps(Ref, msgpack).

putKeySingleOpsPb(Ref) ->
    putKeyNormalOps(Ref, pb).

putKeyNormalOps(Ref) ->
    putKeyNormalOps(Ref, msgpack).

putKeySingleOps(Ref, Encoding) ->
    addKey(Ref, 1, getBin(), Encoding).

putKeyNormalOps(Ref, Encoding) ->
    addKey(Ref, 1, [{<<"f1">>, 1}, {<<"f2">>, <<"test1">>}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}], Encoding),
    addKey(Ref, 2, [{<<"f1">>, 2}, {<<"f2">>, <<"test2">>}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}], Encoding),
    addKey(Ref, 3, [{<<"f1">>, 3}, {<<"f2">>, <<"test3">>}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}], Encoding),
    addKey(Ref, 4, [{<<"f1">>, 4}, {<<"f2">>, <<"test4">>}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}], Encoding).

defaultEvalFn({N,Nmatch}) ->
    (N > 0) and (N == Nmatch).

filterVal({FilterVal}) ->
    FilterVal;
filterVal({FilterVal, _CompVal}) ->
    FilterVal.

eqOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'=', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 == V2 end)).
    
eqEqOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'==', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 == V2 end)).

neqOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'!=', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 /= V2 end)).

gtOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'>', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 > V2 end)).

gteOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'>=', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 >= V2 end)).

ltOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'<', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 < V2 end)).

lteOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'=<', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 =< V2 end)).

allOps(Args) ->
    eqOps(Args) and eqEqOps(Args) and neqOps(Args) and 
	gtOps(Args) and gteOps(Args) and
	ltOps(Args) and lteOps(Args).

eqOpsOnly(Args) ->
    eqOps(Args) and eqEqOps(Args) and neqOps(Args).

allCompOps(Args) ->
    gtOps(Args) and gteOps(Args) and
	ltOps(Args) and lteOps(Args).

anyCompOps(Args) ->
    gtOps(Args) or gteOps(Args) or
	ltOps(Args) or lteOps(Args).

%%------------------------------------------------------------
%% Test timestamp operations
%%------------------------------------------------------------

timestampOps_test() ->
    io:format("timestampOps_test~n"),
    F = <<"f6">>,
    Val = 2000,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = allOps({F, {Val}, timestamp, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test integer operations
%%------------------------------------------------------------

intOps_test() ->
    io:format("intOps_test~n"),
    F = <<"f1">>,
    Val = 3,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = allOps({F, {Val}, integer, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test binary operations
%%------------------------------------------------------------

binaryOps_test() ->
    io:format("binaryOps_test~n"),
    F = <<"f2">>,
    Val = <<"test3">>,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = eqOpsOnly({F, {Val}, binary, PutFn, EvalFn}) and (anyCompOps({F, {Val}, binary, PutFn, EvalFn}) == false),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test float operations
%%------------------------------------------------------------

floatOps_test() ->
    io:format("floatOps_test~n"),
    F = <<"f3">>,
    Val = 3.0,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = allOps({F, {Val}, float, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test boolean operations
%%------------------------------------------------------------

boolOps_test() ->
    io:format("boolOps_test~n"),
    F = <<"f4">>,
    Val = true,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = eqOpsOnly({F, {Val}, boolean, PutFn, EvalFn}) and (anyCompOps({F, {Val}, boolean, PutFn, EvalFn}) == false),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test any operations
%%------------------------------------------------------------

anyOps_test() ->
    io:format("anyOps_test~n"),
    F = <<"f5">>,
    Val = sut:em([1,2,3]),
    CompVal = [1,2,3],
    PutFn  = fun sut:putKeyNormalOps/1,
    EvalFn = fun sut:defaultEvalFn/1,
    Res = eqOpsOnly({F, {Val, CompVal}, any, PutFn, EvalFn}) and (anyCompOps({F, {Val, CompVal}, any, PutFn, EvalFn}) == false),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% All normal operations tests
%%------------------------------------------------------------

normalOpsTests() ->
    intOps_test() and binaryOps_test() and boolOps_test() and floatOps_test() and anyOps_test() and timestampOps_test().

%%=======================================================================
%% Test AND + OR comparators
%%=======================================================================

%%------------------------------------------------------------
%% Test AND operations
%%------------------------------------------------------------

andOps_test() ->
    io:format("andOps_test~n"),
    Cond1 = {'>', {field, <<"f1">>, integer}, {const, 2}},
    Cond2 = {'=', {field, <<"f3">>, float},   {const, 4.0}},
    Filter = {'and_', Cond1, Cond2},
    PutFn  = fun sut:putKeyNormalOps/1,
    Keys = streamFoldTest(Filter, PutFn),
    {N1, NMatch1} = fieldsMatching(Keys, <<"f1">>, 2,   fun(V1,V2) -> V1 > V2 end),
    {N3, NMatch3} = fieldsMatching(Keys, <<"f3">>, 4.0, fun(V1,V2) -> V1 == V2 end),
    Res = (N1 == 1) and (NMatch1 == 1) and (N3 == 1) and (NMatch3 == 1),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test OR filtering
%%------------------------------------------------------------

orOps_test() ->
    io:format("orOps_test~n"),
    Cond1 = {'>', {field, <<"f1">>, integer}, {const, 2}},
    Cond2 = {'=', {field, <<"f3">>, float},   {const, 4.0}},
    Filter = {'or_', Cond1, Cond2},
    PutFn  = fun sut:putKeyNormalOps/1,
    Keys = streamFoldTest(Filter, PutFn),
    {N1, NMatch1} = fieldsMatching(Keys, <<"f1">>, 2,   fun(V1,V2) -> V1 > V2 end),
    {N3, NMatch3} = fieldsMatching(Keys, <<"f3">>, 4.0, fun(V1,V2) -> V1 == V2 end),
    Res = (N1 == 2) and (NMatch1 == 2) and (N3 == 2) and (NMatch3 == 1),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test all AND + OR ops
%%------------------------------------------------------------

andOrOpsTests() ->
    andOps_test() and orOps_test().

%%=======================================================================
%% Abnormal operations testing
%%=======================================================================

%%-----------------------------------------------------------------------
%% Utilities needed for abnormal ops
%%-----------------------------------------------------------------------

putKeyAbnormalOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>,   1}, {<<"f2">>, <<"test1">>}, {<<"f3">>,   1.0}, {<<"f4">>,  false}, {<<"f5">>, em([1,2,3])}, {<<"f6">>,  1000}]),
    addKey(Ref, 2, [{<<"f1">>, 2.1}, {<<"f2">>, <<"test2">>}, {<<"f3">>,   2.0}, {<<"f4">>,   true}, {<<"f5">>, em([2,3,4])}, {<<"f6">>, -2000}]),
    addKey(Ref, 3, [{<<"f1">>,   3}, {<<"f2">>,           3}, {<<"f3">>, "3.0"}, {<<"f4">>,  false}, {<<"f5">>, em([3,4,5])}, {<<"f6">>,  3000}]),
    addKey(Ref, 4, [{<<"f1">>,   4}, {<<"f2">>, <<"test4">>}, {<<"f3">>,   4.0}, {<<"f4">>, "true"}, {<<"f5">>,           4}, {<<"f6">>,  4000}]).

abnormalEvalFn({N,_Nmatch}) ->
    (N == 0).

%%-----------------------------------------------------------------------
%% Test reading data that are not all integers as integers
%%-----------------------------------------------------------------------

badInt_test() ->
    io:format("badInt_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, integer, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading data that are not all timestamps as timestamps
%%-----------------------------------------------------------------------

badTimestamp_test() ->
    io:format("badTimestamp_test~n"),
    F = <<"f6">>,
    Val = 2000,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, timestamp, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading data that are not all binarys as binarys
%%-----------------------------------------------------------------------

badBinary_test() ->
    io:format("badBinary_test~n"),
    F = <<"f2">>,
    Val = <<"test">>,
    PutFn = fun putKeyAbnormalOps/1,
    Res = neqOps({F, {Val}, binary, PutFn, fun({N,_}) -> N == 4 end}),
    ?assert(Res),
    Res.

badBinary2_test() ->
    io:format("badBinary_test~n"),
    F = <<"f2">>,
    Val = <<"test">>,
    PutFn = fun putKeyAbnormalOps/1,
    Res = eqOps({F, {Val}, binary, PutFn, fun({N,_}) -> N == 3 end}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading data that are not all floats as floats
%%-----------------------------------------------------------------------

badFloat_test() ->
    io:format("badFloat_test~n"),
    F = <<"f3">>,
    Val = 0.0,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, float, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading data that are not all booleans as booleans
%%-----------------------------------------------------------------------

badBool_test() ->
    io:format("badBool_test~n"),
    F = <<"f4">>,
    Val = false,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = eqOps({F, {Val}, boolean, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading any data: comparison should succeed!
%%-----------------------------------------------------------------------

badAny_test() ->
    io:format("badAny_test~n"),
    F = <<"f5">>,
    Val = sut:em([1,2,3]),
    CompVal = [1,2,3],
    PutFn  = fun sut:putKeyNormalOps/1,
    Res = neqOps({F, {Val, CompVal}, any, PutFn, fun({N,_}) -> N == 3 end}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% All abnormal ops tests
%%------------------------------------------------------------

abnormalOpsTests() ->
    badInt_test() and badBinary_test() and badFloat_test() and badBool_test() and badTimestamp_test() and badAny_test().

%%=======================================================================
%% Test various exceptional conditions
%%=======================================================================

%%-----------------------------------------------------------------------
%% Utilities needed for exceptional testing
%%-----------------------------------------------------------------------

putKeyMissingOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>, 1}, {<<"f2">>, "test1"}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [{<<"f2">>, "test2"}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>, 3}, {<<"f2">>, "test3"}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>, 4}, {<<"f2">>, "test4"}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

putKeyMissingOps2(Ref) ->
    addKey(Ref, 1, [{<<"f1">>, 1},  {<<"f2">>, "test1"}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [{<<"f1">>, [1]}, {<<"f2">>, "test2"}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>, 3},  {<<"f2">>, "test3"}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>, 4},  {<<"f2">>, "test4"}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

%%------------------------------------------------------------
%% Valid filter, but values are missing for referenced keys
%%------------------------------------------------------------

missingKey_test() ->
    io:format("missingKey_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = gtOps({F, {Val}, integer, PutFn, EvalFn}),
    ?assert(Res),
    Res.

missingKey2_test() ->
    io:format("missingKey_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyMissingOps2/1,
    EvalFn = fun defaultEvalFn/1,
    Res = gtOps({F, {Val}, integer, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Filter references a key that doesn't exist
%%------------------------------------------------------------

filterRefMissingKey_test() ->
    io:format("filterRefMissingKey_test~n"),
    F = <<"f0">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, integer, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Filter specified the wrong type for a valid key
%%------------------------------------------------------------

filterRefWrongType_test() ->
    io:format("filterRefWrongType_test~n"),
    F = <<"f2">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, integer, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Filter specifies type that's not supported
%%------------------------------------------------------------

filterRefInvalidType_test() ->
    io:format("filterRefInvalidType_test~n"),
    F = <<"f2">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, map, PutFn, EvalFn}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% All exceptional tests
%%------------------------------------------------------------

exceptionalTests() ->
    missingKey_test() and filterRefMissingKey_test() and 
	filterRefWrongType_test() and filterRefInvalidType_test().

%%=======================================================================
%% Test scanning
%%=======================================================================

%%-----------------------------------------------------------------------
%% Utilities needed for scanning tests
%%-----------------------------------------------------------------------

streamFoldTestOpts(Opts, FoldFun) ->
    Ref = open(),
    try
	Acc = eleveldb:fold(Ref, FoldFun, [], Opts),
	ok = eleveldb:close(Ref),
	Keys = lists:reverse(Acc)
    catch
	error:Error ->
	    io:format("Caught an error: closing db~n"),
	    ok = eleveldb:close(Ref),
	    error(Error)
    end.

%%------------------------------------------------------------
%% Make sure that we iterate over the right number of keys when
%% iterating over all keys
%%------------------------------------------------------------

scanAll_test() ->
    N = 100,
    putKeysObj(N),
    Opts=[{fold_method, streaming}],
    FoldFun = fun({K,V}, Acc) -> 
		      [getKeyVal(K,V) | Acc]
	      end,
    Keys = streamFoldTestOpts(Opts, FoldFun),
    Len = length(Keys),
    Res = (Len =:= N),
    ?assert(Res),
    Res.

scanAllIteratorTest() ->
    N = 100,
    putKeysObj(N),
    Opts=[{fold_method, iterator}],
    FoldFun = fun({K,V}, Acc) -> 
		      [getKeyVal(K,V) | Acc]
	      end,
    streamFoldTestOpts(Opts, FoldFun).


scanAllStreamingTest() ->
    N = 100,
    putKeysObj(N),
    Opts=[{fold_method, streaming}],
    FoldFun = fun({K,V}, Acc) -> 
		      [getKeyVal(K,V) | Acc]
	      end,
    streamFoldTestOpts(Opts, FoldFun).

%%------------------------------------------------------------
%% Make sure that we iterate over the right number of keys when
%% requesting start and end keys
%%------------------------------------------------------------

scanSome_test() ->
    N = 100,
    putKeysObj(N),
    Opts=[{fold_method, streaming},
	  {start_key, <<"key002">>},
	  {end_key,  <<"key099">>},
	  {end_inclusive,  true}],

    FoldFun = fun({K,_V}, Acc) -> 
		      [K | Acc]
	      end,

    Keys = streamFoldTestOpts(Opts, FoldFun),
    Len = length(Keys),
    Res = (Len =:= N-2),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Don't include the first key
%%------------------------------------------------------------

scanNoStart_test() ->
    N = 100,
    putKeysObj(N),
    Opts=[{fold_method, streaming},
	  {start_inclusive, false},
	  {start_key, <<"key001">>}],

    FoldFun = fun({K,_V}, Acc) -> 
		      [K | Acc]
	      end,

    Keys = streamFoldTestOpts(Opts, FoldFun),
    [Key1 | _Rest] = Keys,
    Len = length(Keys),
    Res = ((Len =:= N-1) and (Key1 =:= <<"key002">>)),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Don't include the start or end key
%%------------------------------------------------------------

scanNoStartOrEnd_test() ->
    N = 100,
    putKeysObj(N),
    Opts=[{fold_method, streaming},
	  {start_inclusive, false},
	  {end_inclusive, false},
	  {start_key, <<"key001">>},
	  {end_key,   <<"key100">>}],

    FoldFun = fun({K,_V}, Acc) -> 
		      [K | Acc]
	      end,

    Keys = streamFoldTestOpts(Opts, FoldFun),

    [KeyFirst | _Rest] = Keys,
    KeyLast = lists:last(Keys),
    
    Len = length(Keys),
    Res = ((Len =:= N-2) and (KeyFirst =:= <<"key002">>) and (KeyLast =:= <<"key099">>)),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% All scanning tests
%%------------------------------------------------------------

scanTests() ->
    scanAll_test() and scanSome_test() and scanNoStart_test() and scanNoStartOrEnd_test().

%%=======================================================================
%% Encoding options tests
%%=======================================================================

%%------------------------------------------------------------
%% This test should throw an exception because no encoding option was
%% specified
%%------------------------------------------------------------

noEncodingOptions_test() ->
    try
	N = 100,
	putKeysObj(N),
	Filter = {'>', {field, <<"f1">>, integer}, {const, 1}},
	Opts=[{fold_method, streaming},
	      {range_filter, Filter}],
	
	FoldFun = fun({K,_V}, Acc) -> 
			  [K | Acc]
		  end,
	
	Keys = streamFoldTestOpts(Opts, FoldFun),
	Len = length(Keys),
	Res = (Len > 0),
	?assert(Res),
	Res
    of
	_ -> 
	    io:format("Function returned ok.  Shouldn't have!"),
	    ?assert(false),
	    false
    catch
	error:Error ->
	    io:format("Caught an error: ~p.  Ok.~n", [Error]),
	    ?assert(true),
	    true
    end.

%%------------------------------------------------------------
%% This test should throw an exception because an invalid encoding
%% option was specified
%%------------------------------------------------------------

badEncodingOptions_test() ->
    try
	N = 100,
	putKeysObj(N),
	Filter = {'>', {field, <<"f1">>, integer}, {const, 1}},
	Opts=[{fold_method, streaming},
	      {range_filter, Filter},
	      {encoding, unknown}],
	
	FoldFun = fun({K,_V}, Acc) -> 
			  [K | Acc]
		  end,
	
	Keys = streamFoldTestOpts(Opts, FoldFun),
	Len = length(Keys),
	Res = (Len > 0),
	?assert(Res),
	Res
    of
	_ -> 
	    io:format("Function returned ok.  Shouldn't have!"),
	    ?assert(false),
	    false
    catch
	error:Error ->
	    io:format("Caught an error: ~p.  Ok.~n", [Error]),
	    ?assert(true),
	    true
    end.

%%------------------------------------------------------------
%% All encoding options tests
%%------------------------------------------------------------

encodingOptionsTests() ->
    noEncodingOptions_test() and badEncodingOptions_test().


%%=======================================================================
%% Test field-field ops
%%=======================================================================

putKeyFieldOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>, 1}, {<<"f2">>, <<"test1">>}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [{<<"f1">>, 2}, {<<"f2">>, <<"test2">>}, {<<"f3">>, 1.5}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>, 3}, {<<"f2">>, <<"test3">>}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>, 4}, {<<"f2">>, <<"test4">>}, {<<"f3">>, 3.5}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

fieldOpsGt_test() ->
    io:format("fieldOpsGt_test~n"),
    Cond1 = {'>', {field, <<"f1">>, float}, {field, <<"f3">>, float}},
    Filter = Cond1,
    PutFn  = fun sut:putKeyFieldOps/1,
    Keys = streamFoldTest(Filter, PutFn),
    {N, NMatch} = fieldsMatching(Keys, <<"f1">>, <<"f3">>,  fun(V1,V2) -> V1 > V2 end),
    Res = (N == 2) and (NMatch == 2),
    ?assert(Res),
    Res.

fieldOpsEq_test() ->
    io:format("fieldOpsEq_test~n"),
    Cond1 = {'=', {field, <<"f1">>, float}, {field, <<"f3">>, float}},
    Filter = Cond1,
    PutFn  = fun sut:putKeyFieldOps/1,
    Keys = streamFoldTest(Filter, PutFn),
    {N, NMatch} = fieldsMatching(Keys, <<"f1">>, <<"f3">>,  fun(V1,V2) -> V1 == V2 end),
    Res = (N == 2) and (NMatch == 2),
    ?assert(Res),
    Res.

fieldOpsLt_test() ->
    io:format("fieldOpsLt_test~n"),
    Cond1 = {'<', {field, <<"f3">>, float}, {field, <<"f1">>, float}},
    Filter = Cond1,
    PutFn  = fun sut:putKeyFieldOps/1,
    Keys = streamFoldTest(Filter, PutFn),
    {N, NMatch} = fieldsMatching(Keys, <<"f3">>, <<"f1">>,  fun(V1,V2) -> V1 < V2 end),
    Res = (N == 2) and (NMatch == 2),
    ?assert(Res),
    Res.

fieldOpsTests() ->
    fieldOpsEq_test() and fieldOpsGt_test() and fieldOpsLt_test().

%%=======================================================================
%% Test const-const ops (for completeness)
%%=======================================================================

constOpsLt_test() ->
    io:format("constOps_test~n"),
    Cond1 = {'<', {const, 1}, {const, 2}},
    Filter = Cond1,
    PutFn  = fun sut:putKeyFieldOps/1,
    Keys = streamFoldTest(Filter, PutFn),
    Res = (length(Keys) == 4),
    ?assert(Res),
    Res.

constOpsGt_test() ->
    io:format("constOpsGt_test~n"),
    Cond1 = {'>', {const, 1}, {const, 2}},
    Filter = Cond1,
    PutFn  = fun sut:putKeyFieldOps/1,
    Keys = streamFoldTest(Filter, PutFn),
    Res = (length(Keys) == 0),
    ?assert(Res),
    Res.

oneIntOpsTest() ->
    {"vm", Val} = eleveldb:statstest(),
    io:format("constOpsGt_test~n"),
%    Cond1 = {'<', {const, 1}, {const, 2}},
%    Filter = Cond1,
    PutFn  = fun sut:putKeyNormalOps/1,
 %   streamFoldTest(Filter, PutFn),
    streamFoldTest({}, PutFn),
    {"vm", Val} = eleveldb:statstest(),
    ok.

oneScanOpsTest() ->
    {"vm", Val} = eleveldb:statstest(),
    scanAll_test(),
    {"vm", Val} = eleveldb:statstest(),
    ok.

constOpsTests() ->
    constOpsGt_test() and constOpsLt_test().

%%------------------------------------------------------------
%% All tests in this file
%%------------------------------------------------------------

allTests() ->
    packObj_test() and normalOpsTests() and abnormalOpsTests() and exceptionalTests() 
	and scanTests()and encodingOptionsTests() and fieldOpsTests() and constOpsTests().

%%=======================================================================
%% Test code to filter & decode TS keys from a leveldb table
%%=======================================================================

readKeysFromTable(Table) ->    

    Cond1 = {'=', {field, <<"user">>, binary},    {const, <<"user_1">>}},
    Cond2 = {'<', {field, <<"time">>, timestamp}, {const, 11000000}},
    Cond3 = {'>', {field, <<"time">>, timestamp}, {const,  9990000}},
    Filter = {'and_', Cond1, {'and_', Cond2, Cond3}},
    Opts=[{fold_method, streaming},
	  {range_filter, Filter},
	  {encoding, msgpack}],
    io:format("Filter = ~p~n", [lists:flatten([Filter])]),
    TableName = "/Users/eml/projects/riak/riak_end_to_end_timeseries/dev/dev1/data/leveldb/" ++ Table,
    io:format("Attemping to open ~ts~n", [TableName]),
    Ref = open(TableName),
    Bucket = {<<"GeoCheckin">>, <<"GeoCheckin">>},
    FoldFun = fun({_K,V}, _Acc) -> 
		      Contents = getKeyVal(Bucket, <<"key">>, V),
		      io:format("Val = ~p~n", [lists:flatten(Contents)])
	      end,

    eleveldb:fold(Ref, FoldFun, [], Opts),
    ok = eleveldb:close(Ref).

r() ->
    readKeysFromTable("1096126227998177188652763624537212264741949407232").

leakTest(N, DoFilter) ->
    putKeysObj(N),
    Ref = open(),

    case DoFilter of
	true ->
	    FieldCond = {'=', {field, <<"f1">>, integer}, {field, <<"f1">>, integer}},
	    Opts=[{fold_method, streaming},
		  {encoding, msgpack},
		  {range_filter, FieldCond}];
	false ->
	    FieldCond = {'!=', {field, <<"f1">>, integer}, {field, <<"f1">>, integer}},
	    Opts=[{fold_method, streaming},
		  {encoding, msgpack},
		  {range_filter, FieldCond}]
    end,

    FF = fun({K,V}, Acc) -> 
		 [getKeyVal(K,V) | Acc]
	 end,

    {"vm", Val1} = eleveldb:statstest(),
    Acc = eleveldb:fold(Ref, FF, [], Opts),
    {"vm", Val2} = eleveldb:statstest(),

    ok = eleveldb:close(Ref),
    lists:reverse(Acc),
    {Val2 - Val1}.

getBin() ->
    [{<<"f1">>, 3}, {<<"f2">>, <<"test">>}, {<<"f3">>, 3}, {<<"f4">>, [1,2,3]}, {<<"f5">>, false}].

msgpackTest() ->
    Bin = msgpack:pack(getBin(), [{format, jsx}]),
    eleveldb:eniftest({msgpacktypeparse, Bin}).

erlangTest() ->
    Bin = term_to_binary(getBin()),
    eleveldb:eniftest({erlangtypeparse, Bin}).

msgOpsTest() ->
    io:format("erlangOps_test~n"),
    Cond1 = {'>', {field, <<"f1">>, integer}, {const, 2}},
    Cond2 = {'=', {field, <<"f3">>, float},   {const, 3.0}},
    Filter = {'and_', Cond1, Cond2},
    PutFn  = fun sut:putKeySingleOpsMsgpack/1,
    Keys = streamFoldTest(Filter, PutFn),
    Keys.

eiOpsTest() ->
    io:format("erlangOps_test~n"),
%    Cond1 = {'>', {field, <<"f1">>, integer}, {const, 2}},
%    Cond2 = {'=', {field, <<"f3">>, float},   {const, 3.0}},
%    Filter = {'and_', Cond1, Cond2},
    Filter = {'=', {field, <<"f2">>, binary}, {const, <<"test3">>}},
    PutFn  = fun sut:putKeySingleOpsEi/1,
    Keys = streamFoldTestErlang(Filter, PutFn),
    Keys.

pbOpsTest() ->
    io:format("pbOps_test~n"),
%    Cond1 = {'>', {field, <<"f1">>, integer}, {const, 2}},
%    Cond2 = {'=', {field, <<"f3">>, float},   {const, 3.0}},
%    Filter = {'and_', Cond1, Cond2},
    Filter = {'=', {field, <<"f2">>, binary}, {const, <<"test3">>}},
    PutFn  = fun sut:putKeySingleOpsPb/1,
    Keys = streamFoldTestPb(Filter, PutFn),
    Keys.

pbTest() ->
    Term = getBin(),
    Enc = eleveldb:eniftest({pbenc, Term}),
    Dec = eleveldb:eniftest({pbdec, Enc}),
    {Term, Dec}.

%=======================================================================
% IO tests
%=======================================================================

clearStreamtest() ->
    os:cmd("rm -rf ./streamtest.out").

%------------------------------------------------------------
% Perform the iterator comparison Niter times for a database of size N                 
%------------------------------------------------------------

comptest(N, Niter) -> 
    comptest(N, Niter, 1).

comptest(N, Niter, Acc) when Acc =< Niter ->
    comptest(N),
    comptest(N, Niter, Acc+1);
comptest(N, Niter, Acc) when Acc > Niter ->
    io:format("Done with ~B~n", [N]),
    ok.

%------------------------------------------------------------
% Perform one iteration of the specified streaming tests
%------------------------------------------------------------

comptest(N) ->

    % Build the database with msgpack encoding

    putKeysObj(N, msgpack),

    {_, T0} = streamTestIterator(msgpack),
    {_, T1} = streamTestStreaming(msgpack),

    {_, T2} = streamTestStreamingFilterTwo(msgpack),
    
    % Rebuild the database with ei encoding

    putKeysObj(N, ei),

    {_, T3} = streamTestStreamingFilterTwo(ei),

    % Rebuild the database with pb encoding

    putKeysObj(N, pb),

    {_, T4} = streamTestStreamingFilterTwo(pb),

    file:write_file("./streamtest.out", io_lib:fwrite("~B ~B ~B ~B ~B ~B~n", [N, T0, T1, T2, T3, T4]), [append]),
    T1/T0.

%------------------------------------------------------------
% Do the whole run
%------------------------------------------------------------

comptestRun(Niter) ->
    sut:comptest(1,     Niter),
    sut:comptest(10,    Niter),
    sut:comptest(50,    Niter),
    sut:comptest(100,   Niter),
    sut:comptest(500,   Niter),
    sut:comptest(1000,  Niter),
    sut:comptest(2000,  Niter),
    sut:comptest(3000,  Niter),
    sut:comptest(4000,  Niter),
    sut:comptest(5000,  Niter),
    sut:comptest(6000,  Niter),
    sut:comptest(7000,  Niter),
    sut:comptest(8000,  Niter),
    sut:comptest(9000,  Niter),
    sut:comptest(10000, Niter).

%------------------------------------------------------------
% Just iterate over all keys in the database, using old-style
% iterators
%------------------------------------------------------------

streamTestTemplate(BaseOpts, Encoding) ->

    Ref = open(),

    % Do nothing with the return values for now -- just test iterating over them

    FF = fun({_K,_V}, Acc) -> Acc end,

    Opts = [{encoding, Encoding} | BaseOpts],
    
    Tstart = eleveldb:current_usec(),
    eleveldb:fold(Ref, FF, [], Opts),
    Tstop = eleveldb:current_usec(),

    ok = eleveldb:close(Ref),
    {0.0, Tstop - Tstart}.

%------------------------------------------------------------
% Just iterate over all keys in the database, using old-style
% iterators
%------------------------------------------------------------

streamTestIterator(Encoding) ->
    Opts=[{fold_method, iterator}],
    streamTestTemplate(Opts, Encoding).

%------------------------------------------------------------
% Just iterate over all keys in the database, using streaming folds
%------------------------------------------------------------

streamTestStreaming(Encoding) ->
    Opts=[{fold_method, streaming}],
    streamTestTemplate(Opts, Encoding).

streamTestStreamingFilterInt(Encoding) ->
    Filter = {'=', {field, <<"f1">>, integer}, {const, 1}},
    Opts=[{fold_method, streaming},
	  {range_filter, Filter}],
    streamTestTemplate(Opts, Encoding).

streamTestStreamingFilterBin(Encoding) ->
    Filter = {'=', {field, <<"f2">>, binary}, {const, <<"test3">>}},
    Opts=[{fold_method, streaming},
	  {range_filter, Filter}],
    streamTestTemplate(Opts, Encoding).

streamTestStreamingFilterTwo(Encoding) ->
    Cond1 = {'=', {field, <<"f1">>, integer}, {const, 1}},
    Cond2 = {'=', {field, <<"f2">>, binary},  {const, <<"test3">>}},
    Filter = {'and', Cond1, Cond2},
    Opts=[{fold_method, streaming},
	  {range_filter, Filter}],
    streamTestTemplate(Opts, Encoding).

streamTestStreamingFilterTwoMaxBatchBytes(Encoding) ->
    Cond1 = {'=', {field, <<"f1">>, integer}, {const, 1}},
    Cond2 = {'=', {field, <<"f2">>, binary},  {const, <<"test3">>}},
    Filter = {'and', Cond1, Cond2},
    Opts=[{fold_method, streaming},
	  {range_filter, Filter}],
    streamTestTemplate(Opts, Encoding).


    


andysTestTemplate(BaseOpts, Encoding) ->

    Ref = open(),

    % Do nothing with the return values for now -- just test iterating over them

    FF = fun({K,V}, Acc) -> 
		 [getKeyVal(K,V) | Acc]
	 end,

    Opts = [{encoding, Encoding} | BaseOpts],
    
    Acc = eleveldb:fold(Ref, FF, [], Opts),

    ok = eleveldb:close(Ref),
    lists:reverse(Acc).

andysTest1() ->
    clearDb(),
    Ref = open(),
    addKey(Ref, 1, [{<<"myfamily">>, <<"myfamily">>}, {<<"myseries">>, <<"simpsons halloween special">>}, {<<"time">>, 44}, {<<"weather">>, <<"cloudy">>}, {<<"temperature">>, 33.4}]),
    addKey(Ref, 2, [{<<"myfamily">>, <<"myfamily">>}, {<<"myseries">>, <<"simpsons halloween special">>}, {<<"time">>, 44}, {<<"weather">>, <<"cloudy">>}, {<<"temperature">>, 33.5}]),
    addKey(Ref, 3, [{<<"myfamily">>, <<"myfamily">>}, {<<"myseries">>, <<"simpsons halloween special">>}, {<<"time">>, 44}, {<<"weather">>, <<"cloudy">>}, {<<"temperature">>, 33.6}]),
    eleveldb:close(Ref),
    Opts=[{fold_method, streaming},
	  {start_inclusive, true}],
    andysTestTemplate(Opts, msgpack).

andysTest2() ->
    clearDb(),
    Ref = open(),
    Filter = {'!=', {field, <<"temperature">>, float}, {const, 33.4}},
    addKey(Ref, 1, [{<<"myfamily">>, <<"myfamily">>}, {<<"myseries">>, <<"simpsons halloween special">>}, {<<"time">>, 44}, {<<"weather">>, <<"cloudy">>}, {<<"temperature">>, 33.4}]),
    addKey(Ref, 2, [{<<"myfamily">>, <<"myfamily">>}, {<<"myseries">>, <<"simpsons halloween special">>}, {<<"time">>, 44}, {<<"weather">>, <<"cloudy">>}, {<<"temperature">>, 33.5}]),
    addKey(Ref, 3, [{<<"myfamily">>, <<"myfamily">>}, {<<"myseries">>, <<"simpsons halloween special">>}, {<<"time">>, 44}, {<<"weather">>, <<"cloudy">>}, {<<"temperature">>, 33.6}]),
    eleveldb:close(Ref),
    Opts=[{fold_method, streaming},
	  {range_filter, Filter},
	  {start_inclusive, true}],
    andysTestTemplate(Opts, msgpack).

testDecoding(<<3:8/integer, _Bin/binary>>) ->
    io:format("Got a 3-integer~n");
testDecoding(<<_Other:8/integer, _Bin/binary>>) ->
    io:format("Got an Other-integer~n");
testDecoding(<<_Bin/binary>>) ->
    io:format("Just got a binary~n").

testEncoding() ->
    testEncoding(<<-2:6>>).
    
testEncoding(Bin) ->
    testDecoding(<<3:8/integer, Bin/binary>>),
    testDecoding(<<2:8/integer, Bin/binary>>),
    testDecoding(<<Bin/binary>>).

    
