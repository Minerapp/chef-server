%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%%% @author Dave Parfitt <dparfitt@chef.io>
%%% @doc
%%% monitor RabbitMQ length of analytics queues
%%% @end
%% Copyright 2011-2015 Chef Software, Inc. All Rights Reserved.
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

-module(chef_wm_actions_queue_monitoring).

-ifdef(TEST).
-compile(export_all).
-endif.

-export([start_link/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         status/0,
         status_for_json/0,
         is_queue_writable/0,
         set_queue_writable/1
        ]).

-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {queue_writable = true, %% Can we write to the queue, or has
                                       %% the queues max-length been exceeded?
                timer,
                max_length = 0,
                last_recorded_length = 0
               }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec status() -> [{atom(), _}].
status() ->
    gen_server:call(?SERVER, status).

status_for_json() ->
    gen_server:call(?SERVER, status_for_json).

is_queue_writable() ->
    gen_server:call(?SERVER, is_queue_writable).

set_queue_writable(Writable) ->
    gen_server:call(?SERVER, {set_queue_writable, Writable}).


%%-------------------------------------------------------------

init([]) ->
    Interval = envy:get(oc_chef_wm, rabbitmq_queue_length_monitor_millis, pos_integer),
    {ok, TRef} = timer:send_interval(Interval, status_ping),
    {ok, #state{timer=TRef}}.

handle_call(is_queue_writable, _From, #state{queue_writable = QueueWritable} = State) ->
    {reply, QueueWritable, State};

handle_call({set_queue_writable, Writable}, _From, State) ->
    lager:info("Manually setting queue writable to ~p", [Writable]),
    {reply, ok, State#state{queue_writable=Writable}};

handle_call(status, _From, State) ->
    {reply, ok, State};
handle_call(status_for_json, _From, State) ->
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(status_ping, State) ->
    lager:debug("Checking server status"),
    check_current_queue_state(State);

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{timer=Timer}) ->
    {ok, cancel} = timer:cancel(Timer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%-------------------------------------------------------------

check_current_queue_state(State) ->
    case get_max_length() of
            % max length isn't configured, or something is broken
            % don't continue.
        undefined -> {noreply, State};
        MaxLength ->
            lager:info("Max Length = ~p", [MaxLength]),
            CurrentLength = get_current_length(),
            case CurrentLength of
                undefined ->
                    lager:info("No messages on queue"),
                    {noreply, State}; % no messages on the queue
                N -> lager:info("Current Length = ~p", [N]),
                     QueueWritable =
                        case CurrentLength == MaxLength of
                          true -> false;
                          false -> true
                        end,
                     {noreply, State#state{max_length = MaxLength,
                                           last_recorded_length = N,
                                            queue_writable = QueueWritable}}
            end
    end.

rabbit_mgmt_server_request(Path) ->
    User = envy:get(oc_chef_wm, rabbitmq_management_user, binary),
    % TODO: Host = ...
    Password = envy:get(oc_chef_wm, rabbitmq_management_password, binary),
    MgmtPort = envy:get(oc_chef_wm, rabbitmq_management_port, non_neg_integer),
    FullUrl = lists:flatten(io_lib:format('http://localhost:~p~s', [MgmtPort, Path])),
    %lager:info("Request to ~p", [FullUrl]),
    ibrowse:send_req(FullUrl,
                     [], get, [], [{basic_auth, {binary_to_list(User),
                                                 binary_to_list(Password)}}]).


get_max_length() ->
    MaxResult = rabbit_mgmt_server_request("/api/policies/%2Fanalytics/max_length"),
    case MaxResult of
        {ok, "200", _, MaxLengthJson} ->
            try
                parse_max_length_response(MaxLengthJson)
            catch _Ex:_Type ->
                      lager:error("Invalid RabbitMQ response")
            end;
        {error, {conn_failed,_}} ->
            lager:info("Can't connect to RabbitMQ management console"),
            undefined;
        {ok, "404", _, _} ->
            lager:info("RabbitMQ max-length policy not set"),
            undefined;
        _Resp -> lager:error("Unknown response from RabbitMQ management console"),
                 undefined

    end.

get_current_length() ->
    CurrentResult = rabbit_mgmt_server_request("/api/queues/%2Fanalytics"),
    case CurrentResult of
        {error, {conn_failed,_}} ->
            % this is a lager:debug so it won't print out every time the timer
            % fires
            lager:info("Can't connect to RabbitMQ management console"),
            undefined;
        {ok, "200", _, CurrentStatusJson} ->
            try
                parse_current_length_response(CurrentStatusJson)
            catch _:_ ->
                      lager:error("Invalid RabbitMQ response")
            end;
        _Resp -> lager:error("Unknown response from RabbitMQ management console"),
                 undefined
    end.

parse_current_length_response(CurrentStatusJson) ->
    CurrentJSON = jiffy:decode(CurrentStatusJson),

    % make a proplists of each queue and it's current length
    QueueLengths =
    lists:map(fun (QueueStats) -> {QS} = QueueStats,
                                    {proplists:get_value(<<"name">>, QS),
                                    proplists:get_value(<<"messages">>, QS)}
                end, CurrentJSON),
    %%lager:info("Qs ~p", [QueueLengths]),
    % look for the alaska queue length
    proplists:get_value(<<"alaska">>, QueueLengths, undefined).

parse_max_length_response(MaxLengthJson) ->
    {MaxLengthPolicy} = jiffy:decode(MaxLengthJson),
    {Defs} = proplists:get_value(<<"definition">>, MaxLengthPolicy),
    proplists:get_value(<<"max-length">>, Defs, undefined).
