%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author John Keiser <jkeiser@opscode.com>
%% @doc Helper module for calling various Chef REST endpoints
%% @end
%%
%% Copyright 2011-2012 Opscode, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%

-module(chef_solr).

-export([
         add_org_guid_to_query/2,
         delete_search_db/1,
         delete_search_db_by_type/2,
         make_query_from_params/4,
         ping/0,
         search/1,
         search_provider/0,
         id_field/0,
         database_field/0,
         type_field/0,
         solr_commit/0,
         update_url/0
        ]).

-include("chef_solr.hrl").

search_provider() ->
    envy:get(chef_index, search_provider, solr, envy:one_of([solr, cloudsearch])).

id_field() ->
    id_field(search_provider()).

id_field(solr) ->
    <<"X_CHEF_id_CHEF_X">>;
id_field(cloudsearch) ->
    <<"x_chef_id_chef_x">>.

database_field() ->
    database_field(search_provider()).

database_field(solr) ->
    <<"X_CHEF_database_CHEF_X">>;
database_field(cloudsearch) ->
    <<"x_chef_database_chef_x">>.

type_field() ->
    type_field(search_provider()).

type_field(solr) ->
    <<"X_CHEF_type_CHEF_X">>;
type_field(cloudsearch) ->
    <<"x_chef_type_chef_x">>.

-spec make_query_from_params(binary()|string(),
                             string() | binary() | undefined,
                             string(),
                             string()) -> #chef_solr_query{}.
make_query_from_params(ObjType, QueryString, Start, Rows) ->
    % TODO: super awesome error messages
    FilterQuery = make_fq_type(ObjType),
    %% 'sort' param is ignored and hardcoded because indexing
    %% scheme doesn't support sorting since there is only one field.
    Sort = binary_to_list(id_field()) ++ " asc",
    #chef_solr_query{query_string = check_query(QueryString),
                     filter_query = FilterQuery,
                     search_provider = search_provider(),
                     start = decode({nonneg_int, "start"}, Start, 0),
                     rows = decode({nonneg_int, "rows"}, Rows, 1000),
                     sort = Sort,
                     index = index_type(ObjType)}.

-spec add_org_guid_to_query(#chef_solr_query{}, binary()) ->
                                   #chef_solr_query{}.
add_org_guid_to_query(Query = #chef_solr_query{filter_query = FilterQuery,
                                               search_provider = solr},
                      OrgGuid) ->
    Query#chef_solr_query{filter_query = "+" ++
                              search_db_from_orgid(OrgGuid) ++
                              " " ++ FilterQuery};
add_org_guid_to_query(Query = #chef_solr_query{filter_query = FilterQuery,
                                               search_provider = cloudsearch},
                      OrgGuid) ->
    Query#chef_solr_query{filter_query = sq_and(
                                           sq_term(binary_to_list(database_field()),
                                                   db_from_orgid(OrgGuid)),
                                           FilterQuery)}.

sq_term(Field, Value) ->
    "term field=" ++ Field ++ " " ++ sq_quote(Value).

sq_and(First, Second) ->
    "(and (" ++ First  ++ ")(" ++ Second ++ "))".

sq_quote(Value) ->
    "'" ++ Value ++ "'".


response_field() ->
    response_field(search_provider()).

response_field(solr) ->
    <<"response">>;
response_field(cloudsearch) ->
    <<"hits">>.

num_found_field() ->
    num_found_field(search_provider()).

num_found_field(solr) ->
    <<"numFound">>;
num_found_field(cloudsearch) ->
    <<"found">>.

docs_field() ->
    docs_field(search_provider()).

docs_field(solr) ->
    <<"docs">>;
docs_field(cloudsearch) ->
    <<"hit">>.

maybe_unwrap_doc(DocList) ->
    maybe_unwrap_doc(DocList, search_provider()).

maybe_unwrap_doc(DocList, solr) ->
    DocList;
maybe_unwrap_doc(DocList, cloudsearch) ->
    [ej:get({<<"fields">>}, Doc) || Doc <- DocList].

-spec search(#chef_solr_query{}) ->
                    {ok, non_neg_integer(), non_neg_integer(), [binary()]} |
                    {error, {solr_400, string()}} |
                    {error, {solr_500, string()}}.
search(#chef_solr_query{} = Query) ->
    %% FIXME: error handling
    Url = make_solr_query_url(Query),
    {ok, Code, _Head, Body} = chef_index_http:request(Url, get, []),
    case Code of
        "200" ->
            SolrData = jiffy:decode(Body),
            Response = ej:get({response_field()}, SolrData),
            Start = ej:get({<<"start">>}, Response),
            NumFound = ej:get({num_found_field()}, Response),
            DocList = ej:get({docs_field()}, Response),
            Ids = [ ej:get({id_field()}, Doc) || Doc <- maybe_unwrap_doc(DocList) ],
            {ok, Start, NumFound, Ids};
        %% We only have the transformed query at this point, so for the following two error
        %% conditions, we just send along the full query URL. This is for logging only and
        %% should NOT be sent back to the client. Note that a 400 from solr can occur when
        %% the query is bad or when something that ends up in the filter query parameter is
        %% bad, for example, an index with special characters.
        "400" ->
            {error, {solr_400, Url}};
        "500" ->
            {error, {solr_500, Url}}
    end.

-spec ping() -> pong | pang.
ping() ->
    try
        %% FIXME: solr will barf on doubled '/'s so SolrUrl must not end with a trailing slash
        case chef_index_http:request(ping_url(), get, []) of
            %% FIXME: verify that solr returns non-200 if something is wrong and not "status":"ERROR".
            {ok, "200", _Head, _Body} -> pong;
            _Error -> pang
        end
    catch
        How:Why ->
            error_logger:error_report({chef_solr, ping, How, Why}),
            pang
    end.

ping_url() ->
    ping_url(search_provider()).

ping_url(solr) ->
    "/admin/ping?wt=json";
ping_url(cloudsearch) ->
    "/search".

%% TODO: Deal properly with errors
%% @doc Delete all search index entries for a given organization.
-spec delete_search_db(OrgId :: binary()) -> ok.
delete_search_db(OrgId) ->
    DeleteQuery = "<?xml version='1.0' encoding='UTF-8'?><delete><query>" ++
        search_db_from_orgid(OrgId) ++
        "</query></delete>",
    ok = solr_update(DeleteQuery),
    ok = solr_commit(),
    ok.

%% @doc Delete all search index entries for a given
%% organization and type.  Types are generally binaries or strings elsewhere in this
%% module. We should think about converting the other APIs in this file to use atoms
%% instead.
%% Note: This omits solr_commit because of the high cost of that call in production.
%% Some users will want to call the commit directly.
%% @end
-spec delete_search_db_by_type(OrgId :: binary(), Type :: atom()) -> ok.
delete_search_db_by_type(OrgId, Type)
  when Type == client orelse Type == data_bag_item orelse
       Type == environment orelse Type == node orelse
       Type == role ->
    DeleteQuery = "<?xml version='1.0' encoding='UTF-8'?><delete><query>" ++
        search_db_from_orgid(OrgId) ++ " AND " ++
        search_type_constraint(Type) ++
        "</query></delete>",
    solr_update(DeleteQuery).

%% Internal functions

%% @doc Generates the name of the organization's search database from its ID
%% @end
%%
%% Note: this really returns a string(), but Dialyzer is convinced it's a byte list (which
%% it is, technically).  In order for it to be recognized as a printable string, though,
%% we'd have to use io_lib:format
-spec search_db_from_orgid(OrgId :: binary()) -> DBName :: [byte(),...].
search_db_from_orgid(OrgId) ->
    binary_to_list(database_field()) ++ ":" ++ db_from_orgid(OrgId).

db_from_orgid(OrgId) ->
    "chef_" ++ binary_to_list(OrgId).

%% @doc Generates a constraint for chef_type
%% @end
-spec search_type_constraint(Type :: atom()) -> TypeConstraint :: [byte(),...].
search_type_constraint(Type) ->
    binary_to_list(type_field()) ++ atom_to_list(Type).

% /solr/select?
    % fq=%2BX_CHEF_type_CHEF_X%3Anode+%2BX_CHEF_database_CHEF_X%3Achef_288da1c090ff45c987346d2829257256
    % &indent=off
    % &q=content%3Aattr1__%3D__v%2A
-spec make_solr_query_url(#chef_solr_query{}) -> string().
make_solr_query_url(Query = #chef_solr_query{
                               search_provider = Provider,
                               filter_query = FilterQuery}) ->
    %% ensure we filter on an org ID
    assert_org_id_filter(FilterQuery, Provider),
    Url = search_url_fmt(Provider),
    Args = search_url_args(Query),
    lists:flatten(io_lib:format(Url, Args)).

search_url_fmt(solr) ->
    "/select?"
        "fq=~s"
        "&indent=off"
        "&q=~s"
        "&start=~B"
        "&rows=~B"
        "&wt=json"
        "&sort=~s";
search_url_fmt(cloudsearch) ->
    "/search?"
        "fq=~s"
        "&q=~s"
        "&q.parser=lucene"
        "&start=~B"
        "&sort=~s".

search_url_args(#chef_solr_query{
                   query_string = Query,
                   search_provider = solr,
                   filter_query = FilterQuery,
                   start = Start,
                   rows = Rows,
                   sort = Sort}) ->
    [ibrowse_lib:url_encode(FilterQuery),
     ibrowse_lib:url_encode(Query),
     Start, Rows,
     ibrowse_lib:url_encode(Sort)];
search_url_args(#chef_solr_query{
                   query_string = Query,
                   search_provider = cloudsearch,
                   filter_query = FilterQuery,
                   start = Start,
                   sort = Sort}) ->
    [ibrowse_lib:url_encode(FilterQuery),
     ibrowse_lib:url_encode(Query),
     Start, ibrowse_lib:url_encode(Sort)].

assert_org_id_filter(FieldQuery, solr) ->
    Start = "+" ++ binary_to_list(database_field()) ++ ":chef_",
    Len = length(Start),
    assert_equal(Start, string:substr(FieldQuery, 1, Len));
assert_org_id_filter(FieldQuery, cloudsearch) ->
    Start = "(and (term field=" ++ binary_to_list(database_field()) ++ " 'chef_",
    Len = length(Start),
    assert_equal(Start, string:substr(FieldQuery, 1, Len)).

assert_equal(A, A) ->
    ok.

make_fq_type(ObjType) when is_binary(ObjType) ->
    make_fq_type(binary_to_list(ObjType));
make_fq_type(ObjType) ->
    make_fq_type(ObjType, search_provider()).

make_fq_type(ObjType, solr) when ObjType =:= "node";
                           ObjType =:= "role";
                           ObjType =:= "client";
                           ObjType =:= "environment" ->
    "+" ++ binary_to_list(type_field()) ++ ":" ++ ObjType;
make_fq_type(ObjType, solr) ->
    "+" ++ binary_to_list(type_field()) ++ ":data_bag_item +data_bag:" ++ ObjType;
make_fq_type(ObjType, cloudsearch) when ObjType =:= "node";
                           ObjType =:= "role";
                           ObjType =:= "client";
                           ObjType =:= "environment" ->
    sq_term(binary_to_list(type_field()), ObjType);
make_fq_type(ObjType, cloudsearch) ->
    sq_term(binary_to_list(type_field()), "data_bag_item +data_bag:" ++ ObjType).

index_type(Type) when is_binary(Type) ->
    index_type(binary_to_list(Type));
index_type("node") ->
    'node';
index_type("role") ->
    'role';
index_type("client") ->
    'client';
index_type("environment") ->
    'environment';
index_type(DataBag) ->
    {'data_bag', list_to_binary(DataBag)}.

check_query(RawQuery) ->
    case RawQuery of
        undefined ->
            %% Default query string if no 'q' param is present. We might
            %% change this to be a 400 in the future.
            "*:*";
        "" ->
            %% thou shalt not query with the empty string
            throw({bad_query, ""});
        Query ->
            transform_query(http_uri:decode(Query))
    end.

transform_query(RawQuery) when is_list(RawQuery) ->
    transform_query(list_to_binary(RawQuery));
transform_query(RawQuery) ->
    case chef_lucene:parse(RawQuery) of
        Query when is_binary(Query) ->
            binary_to_list(Query);
        _ ->
            throw({bad_query, RawQuery})
    end.

decode({nonneg_int, Key}, Val, Default) ->
    {Int, Orig} =
        case Val of
            undefined ->
                {Default, default};
            Value ->
                try
                    {list_to_integer(http_uri:decode(Value)), Value}
                catch
                    error:badarg ->
                        throw({bad_param, {Key, Value}})
                end
        end,
    validate_non_neg(Key, Int, Orig).

validate_non_neg(Key, Int, OrigValue) when Int < 0 ->
    throw({bad_param, {Key, OrigValue}});
validate_non_neg(_Key, Int, _OrigValue) ->
    Int.

%%------------------------------------------------------------------------------
%% Direct Solr Server Interaction
%%
%% To drop all entries for a given org's search "database", we need to bypass the indexer
%% queue and interact directly with the Solr server.  These functions facilitate that.
%%------------------------------------------------------------------------------

%% @doc Sends `Body` to the Solr server's "/update" endpoint.
%% @end
%%
%% Body is really a string(), but Dialyzer can only determine it is a list of bytes due to
%% the implementation of search_db_from_orgid/1
-spec solr_update(Body :: [byte(),...]) -> ok | {error, term()}.
solr_update(Body) ->
    try
        %% FIXME: solr will barf on doubled '/'s so SolrUrl must not end with a trailing slash
        case chef_index_http:request(update_url(), post, Body) of
            %% FIXME: verify that solr returns non-200 if something is wrong and not "status":"ERROR".
            {ok, "200", _Head, _Body} -> ok;
            Error -> {error, Error}
        end
    catch
        How:Why ->
            error_logger:error_report({chef_solr, update, How, Why}),
            {error, Why}
    end.

update_url() ->
    update_url(search_provider()).

update_url(solr) ->
    "/update";
update_url(cloudsearch) ->
    "/documents/batch".

%% @doc Sends a "commit" message directly to Solr
%% This is exposed for the users of delete_search_db_by_type
-spec solr_commit() -> ok | {error, term()}.
solr_commit() ->
    solr_commit(search_provider()).

solr_commit(solr) ->
    solr_update("<?xml version='1.0' encoding='UTF-8'?><commit/>");
solr_commit(cloudserch) ->
    lager:info("Commit not supported when using cloudsearch as a search provider"),
    ok.
