%%==============================================================================
%% The 'signatures' table
%%==============================================================================

-module(els_dt_signatures).

%%==============================================================================
%% Behaviour els_db_table
%%==============================================================================

-behaviour(els_db_table).
-export([ name/0
        , opts/0
        ]).

%%==============================================================================
%% API
%%==============================================================================

-export([ insert/1
        , lookup/1
        ]).

%%==============================================================================
%% Includes
%%==============================================================================
-include("erlang_ls.hrl").

%%==============================================================================
%% Item Definition
%%==============================================================================

-record(els_dt_signatures, { mfa  :: mfa()  | '_'
                           , spec :: binary()
                           }).
-type els_dt_signatures() :: #els_dt_signatures{}.

-type item() :: #{ mfa  := mfa()
                 , spec := binary()
                 }.
-export_type([ item/0 ]).

%%==============================================================================
%% Callbacks for the els_db_table Behaviour
%%==============================================================================

-spec name() -> atom().
name() -> ?MODULE.

-spec opts() -> proplists:proplist().
opts() ->
  [set].

%%==============================================================================
%% API
%%==============================================================================

-spec from_item(item()) -> els_dt_signatures().
from_item(#{ mfa := MFA, spec := Spec }) ->
  #els_dt_signatures{ mfa = MFA, spec = Spec }.

-spec to_item(els_dt_signatures()) -> item().
to_item(#els_dt_signatures{ mfa = MFA, spec = Spec }) ->
  #{ mfa  => MFA
   , spec => Spec
   }.

-spec insert(item()) -> ok | {error, any()}.
insert(Map) when is_map(Map) ->
  Record = from_item(Map),
  els_eleveldb:write(name(), Record).

-spec lookup(mfa()) -> {ok, [item()]}.
lookup(MFA) ->
  {ok, Items} = els_eleveldb:lookup(name(), MFA),
  {ok, [to_item(Item) || Item <- Items]}.
