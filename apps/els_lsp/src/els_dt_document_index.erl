%%==============================================================================
%% The 'document_index' table
%%==============================================================================

-module(els_dt_document_index).

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

-export([new/3]).

-export([ find_by_kind/1
        , insert/1
        , lookup/1
        ]).

%%==============================================================================
%% Includes
%%==============================================================================
-include("els_lsp.hrl").

%%==============================================================================
%% Item Definition
%%==============================================================================

-record(els_dt_document_index, { id   :: els_dt_document:id()   | '_'
                               , uri  :: uri()                  | '_'
                               , kind :: els_dt_document:kind() | '_'
                               }).
-type els_dt_document_index() :: #els_dt_document_index{}.

-type item() :: #{ id   := els_dt_document:id()
                 , uri  := uri()
                 , kind := els_dt_document:kind()
                 }.
-export_type([ item/0 ]).

%%==============================================================================
%% Callbacks for the els_db_table Behaviour
%%==============================================================================

-spec name() -> atom().
name() -> ?MODULE.

-spec opts() -> proplists:proplist().
opts() ->
  [bag].

%%==============================================================================
%% API
%%==============================================================================

-spec from_item(item()) -> els_dt_document_index().
from_item(#{id := Id, uri := Uri, kind := Kind}) ->
  #els_dt_document_index{id = Id, uri = Uri, kind = Kind}.

-spec to_item(els_dt_document_index()) -> item().
to_item(#els_dt_document_index{id = Id, uri = Uri, kind = Kind}) ->
  #{id => Id, uri => Uri, kind => Kind}.

-spec insert(item()) -> ok | {error, any()}.
insert(Map) when is_map(Map) ->
  Record = from_item(Map),
  Key = {Record#?MODULE.id, make_ref()},
  Res = els_eleveldb:write(name(), Key, Record),
  index_id(Record#?MODULE.id, Key),
  index_kind(Record#?MODULE.kind, Key),
  Res.

-spec lookup(els_dt_document:id()) -> {ok, [item()]}.
lookup(Id) ->
  find_by(Id).

-spec find_by_kind(els_dt_document:kind()) -> {ok, [item()]}.
find_by_kind(Kind) ->
  find_by(Kind).

-spec find_by(els_dt_document:kind() | els_dt_document:id()
                  ) -> {ok, [item()]}.
find_by(IdxKey) ->
  Items = els_eleveldb:lookup_idx(name(), IdxKey),
  {ok, [to_item(Item) || Item <- Items]}.

-spec new(atom(), uri(), els_dt_document:kind()) -> item().
new(Id, Uri, Kind) ->
  #{ id   => Id
   , uri  => Uri
   , kind => Kind
   }.

-spec index_id(atom(), {any(), reference()}) -> ok.
index_id(Id, Key) ->
  els_eleveldb:add_idx(name(), Id, Key).

-spec index_kind(els_dt_document:kind(), {any(), reference()}) ->
        ok | {error, any()}.
index_kind(Kind, Key) ->
  els_eleveldb:add_idx(name(), Kind, Key).
