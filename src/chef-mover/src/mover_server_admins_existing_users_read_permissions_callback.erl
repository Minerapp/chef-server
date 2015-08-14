%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Tyler Cloke <tyler@chef.io>
%% @copyright 2015 Chef Software, Inc.
%%
%% This migration iterates through all existing users and grants
%% the server-admins global group READ access on them, as having
%% access on the container does not update permissions for
%% existing users.

-module(mover_server_admins_existing_users_read_permissions_callback).

-export([
         migration_init/0,
         migration_complete/0,
         migration_type/0,
         supervisor/0,
         migration_start_worker_args/2,
         migration_action/2,
         next_object/0,
         error_halts_migration/0,
         reconfigure_object/2,
         needs_account_dets/0
        ]).

-define(GLOBAL_PLACEHOLDER_ORG_ID, <<"00000000000000000000000000000000">>).

-record(user, {username, authz_id}).
-record(group, {authz_id}).

migration_init() ->
    mv_oc_chef_authz_http:create_pool(),
    put(server_admins_authz_id, get_server_admins_authz_id()),
    put(authz_superuser_id, mv_oc_chef_authz:superuser_id()),
    mover_transient_migration_queue:initialize_queue(?MODULE, get_users()).

migration_action(UserRecord, _AcctInfo) ->
    Username = UserRecord#user.username,
    UserAuthzid = UserRecord#user.authz_id,
    case mv_oc_chef_authz:add_ace_for_entity(get(authz_superuser_id), group, get(server_admins_authz_id) , actor, UserAuthzid, read) of
	{error, Error} ->
	    lager:error("Failed to update read permission for user " ++ Username ++ " with error:"),
	    lager:error(Error),
	    throw(migration_error);
	_ ->
	    ok
    end.

get_users() ->
    {ok, Users} = sqerl:select(get_users_sql(), [], rows_as_records, [user, record_info(fields, user)]),
    Users.

get_users_sql() ->
    <<"SELECT username, authz_id FROM users">>.

get_server_admins_authz_id() ->
    {ok, [ServerAdmin]} = sqerl:select(get_server_admins_authz_id_sql(), [], rows_as_records, [group, record_info(fields, group)]),
    ServerAdmin#group.authz_id.

get_server_admins_authz_id_sql() ->
    erlang:iolist_to_binary([<<"SELECT authz_id FROM groups WHERE name='server-admins' and org_id='">>, ?GLOBAL_PLACEHOLDER_ORG_ID , <<"'">>]).

migration_complete() ->
    mv_oc_chef_authz_http:delete_pool().

%%
%% Generic mover callback functions for
%% a transient queue migration
%%
needs_account_dets() ->
    false.

migration_start_worker_args(Object, AcctInfo) ->
    [Object, AcctInfo].

next_object() ->
    mover_transient_migration_queue:next(?MODULE).

migration_type() ->
    <<"users_read_access_on_server_admins">>.

supervisor() ->
    mover_transient_worker_sup.

error_halts_migration() ->
    true.

reconfigure_object(_ObjectId, _AcctInfo) ->
    ok.
