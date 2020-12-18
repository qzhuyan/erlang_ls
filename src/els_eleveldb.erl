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
        ]).

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
  eleveldb:delete(handler(Table), term_to_binary(Key), []).

-spec delete_object(atom(), any()) -> ok.
delete_object(Table, Object) ->
  delete(Table, element(2, Object)).

-spec lookup(atom(), any()) -> {ok, [tuple()]}.
lookup(Table, Key) ->
  Res = try eleveldb:get(handler(Table), term_to_binary(Key), []) of
          {ok, Bin} ->
            [binary_to_term(Bin)];
          not_found -> []
        catch  badarg:_ ->
            []
        end,
  {ok, Res}.

-spec match(atom(), tuple()) -> {ok, [tuple()]}.
match(Table, Pattern) when is_tuple(Pattern) ->
  MS= [{Pattern, [], ['$_']}],
  {ok, do_match(handler(Table), MS)}.

-spec match_delete(atom(), tuple()) -> ok.
match_delete(Table, Pattern) when is_tuple(Pattern) ->
  %% @todo use fold
  {ok, Matched} = match(Table, Pattern),
  case handler(Table) of
    undefined -> ok;
    H ->
      lists:foreach(fun(X) ->
                        delete(H, term_to_binary(element(2, X))) end,
                    Matched)
  end,
  ok.

-spec write(atom(), tuple()) -> ok.
write(Table, Object) when is_tuple(Object) ->
  Key = term_to_binary(element(2, Object)),
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

-spec do_match(reference(), tuple()) -> {ok, [any()]}.
do_match(Tab, MS) ->
  CMS = ets:match_spec_compile(MS),
  InitAcc = {[], [], CMS}, %% {Res, Buff}
  {Res, Buff, CMS} = eleveldb:fold(Tab, fun match_fun/2, InitAcc, []),
  NewHits = ets:match_spec_run(Buff, CMS),
  NewHits ++ Res.

-spec match_fun({binary(), binary()}, any()) -> any().
match_fun({K, V}, {Res, Buff, CMS}) when length(Buff) > 1000 ->
  NewHits = ets:match_spec_run(Buff, CMS),

  match_fun({K, V}, {NewHits ++ Res, [], CMS});
match_fun({_K, V}, {Res, Buff, CMS}) ->
  {Res, [binary_to_term(V) | Buff], CMS}.

-spec handler(atom()) -> any().
handler(Table) ->
  case ets:lookup(eleveldb_handler, Table) of
    [{_, H}] -> H;
    [] -> undefined
  end.

-spec name(atom()) -> string().
name(Table) ->
  filename:join(["/tmp", node(), Table]).

-spec open(atom()) -> ok.
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

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
