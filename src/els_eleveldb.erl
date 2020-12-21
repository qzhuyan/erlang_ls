-module(els_eleveldb).
%% API
-export([ clear_table/1
        , clear_tables/0
        , delete/2
        , delete_object/2
        , lookup/2
        , match/2
        , match_delete/2
        , tables/0
        , write/2
        , name/1
        , open/1
        , write/3
        ]).

-define(MS(Pattern), [{Pattern, [], ['$_']}]).

%%==============================================================================
%% Exported functions
%%==============================================================================

-spec tables() -> [atom()].
tables() ->
  [ els_dt_document
  , els_dt_document_index
  , els_dt_references
  , els_dt_signatures
  ].

-spec delete(atom(), any()) -> ok.
delete(Table, Key) ->
  case handler(Table) of
    undefined -> ok;
    H ->
      eleveldb:delete(H, term_to_binary(Key), [])
  end.

-spec delete_object(atom(), any()) -> ok.
delete_object(Table, Object) ->
  delete(Table, element(2, Object)).

-spec lookup(atom(), any()) -> {ok, [tuple()]}.
lookup(Table, Key) ->
  Res = case eleveldb:get(handler(Table), term_to_binary(Key), []) of
          {ok, Bin} ->
            [binary_to_term(Bin)];
          not_found -> []
        end,
  {ok, Res}.

-spec match(atom(), tuple()) -> {ok, [tuple()]}.
match(Table, Pattern) when is_tuple(Pattern) ->
  MS = ?MS(Pattern),
  {ok, do_match(handler(Table), MS)}.

-spec match_delete(atom(), tuple()) -> ok.
match_delete(Table, Pattern) when is_tuple(Pattern) ->
  MS = ?MS(Pattern),
  do_match_delete(handler(Table), MS).

-spec write(atom(), tuple()) -> ok.
write(Table, Object) when is_tuple(Object) ->
  Key = term_to_binary(element(2, Object)),
  Val = term_to_binary(Object),
  ok = eleveldb:put(handler(Table), Key, Val, []).

-spec write(atom(), any(), tuple()) -> ok.
write(Table, Key0, Object) when is_tuple(Object) ->
  Key = term_to_binary(Key0),
  Val = term_to_binary(Object),
  ok = eleveldb:put(handler(Table), Key, Val, []).

-spec clear_table(atom()) -> ok.
clear_table(Table) ->
  ok = eleveldb:close(handler(Table)),
  true = ets:delete(eleveldb_handler, Table),
  ok = eleveldb:destroy(name(Table), []),
  {ok, _} = open(Table),
  ok.

-spec clear_tables() -> ok.
clear_tables() ->
  [ok = clear_table(T) || T <- tables()],
  ok.

-spec do_match(eleveldb:db_ref(), ets:match_spec()) -> [any()].
do_match(H, MS) ->
  CMS = ets:match_spec_compile(MS),
  InitAcc = {[], [], CMS},
  {Res, Buff, CMS} = eleveldb:fold(H, fun match_fun/2, InitAcc, []),
  NewHits = ets:match_spec_run(Buff, CMS),
  NewHits ++ Res.

-spec do_match_delete(eleveldb:db_ref(), ets:match_spec()) -> ok.
do_match_delete(Handler, MS) ->
  CMS = ets:match_spec_compile(MS),
  Batch = eleveldb:fold(Handler, fun({K, V}, Acc) ->
                         case ets:match_spec_run([binary_to_term(V)], CMS) of
                           [] -> Acc;
                           _Hit ->
                             [{delete, K} | Acc]
                         end
                     end, [], []),
  ok = eleveldb:write(Handler, Batch, []).

-spec match_fun({binary(), binary()}, any()) -> any().
match_fun({K, V}, {Res, Buff, CMS}) when length(Buff) > 1000 ->
  NewHits = ets:match_spec_run(Buff, CMS),

  match_fun({K, V}, {NewHits ++ Res, [], CMS});
match_fun({_K, V}, {Res, Buff, CMS}) ->
  {Res, [binary_to_term(V) | Buff], CMS}.

-spec handler(atom()) -> undefined | eleveldb:db_ref().
handler(Table) ->
  case ets:lookup(eleveldb_handler, Table) of
    [{_, H}] -> H;
    [] -> undefined
  end.

-spec name(atom()) -> string().
name(Table) ->
  ProjRoot = get_proj_root(),
  filename:join([ProjRoot, "els_cache", leveldb, Table]).

-spec open(atom()) -> {ok, eleveldb:db_ref()} | {error, any()}.
open(Table) ->
  DirName = name(Table),
  filelib:ensure_dir(DirName),
  case eleveldb:open(DirName, [{create_if_missing, true}]) of
    {ok, H} ->
      ets:insert(eleveldb_handler, {Table, H}),
      {ok, H};
    Other ->
      Other
  end.

-spec get_proj_root() -> file:name().
get_proj_root() ->
  binary_to_list(els_uri:path(els_config:get(root_uri))).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
