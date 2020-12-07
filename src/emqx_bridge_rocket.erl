%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:03
%%%-------------------------------------------------------------------

-module(emqx_bridge_rocket).

-include("../include/emqx_bridge_rocket.hrl").
-include("../include/emqx.hrl").

-export([load/1, unload/0]).

-export([register_metrics/0]).

-export([rocket_callback/2]).

-export([on_client_connected/3,
  on_client_disconnected/4,
  on_session_subscribed/4,
  on_session_unsubscribed/4,
  on_message_publish/2,
  on_message_delivered/3,
  on_message_acked/3]).

-import(proplists, [get_value/2]).

-vsn("4.2.1").

register_metrics() ->
  [emqx_metrics:new(MetricName) || MetricName
    <- ['bridge.rocket.client_connected', %% 连接
      'bridge.rocket.client_disconnected',%% 断开连接
      'bridge.rocket.session_subscribed', %% 订阅
      'bridge.rocket.session_unsubscribed',%% 取消订阅
      'bridge.rocket.message_publish',%% 发布
      'bridge.rocket.message_acked',%% 消息回复确认
      'bridge.rocket.message_delivered']].%% 消息投递

%% 载入
load(ClientId) ->
  HookList = parse_hook(application:get_env(emqx_bridge_rocket, hooks, [])),
  %% socket 配置
  SockOpts = application:get_env(emqx_bridge_rocket, sock_opts, []),
  %% 刷新路由时间间隔
  RefTopicRouteInterval = application:get_env(emqx_bridge_rocket, ref_topic_route_interval, 5000),
  %% 批处理消息大小设置
  BatchSize = application:get_env(emqx_bridge_rocket, batch_size, 0),
  %% 生产者配置
  ProducerOpts = #{tcp_opts => SockOpts, ref_topic_route_interval => RefTopicRouteInterval,
    callback => fun emqx_bridge_rocket:rocket_callback/2, batch_size => BatchSize},
  io:format("~s is loaded.~n", [emqx_bridge_rocket]),
  lists:foldl(fun ({Hook, Filter, Topic, PayloadFormat}, Acc) ->
    %% 生产者组
    ProducerGroup = list_to_binary(lists:concat([ClientId, "_", binary_to_list(Topic)])),
    %% 配置
    ProducerOpts1 = ProducerOpts#{name => binary_to_atom(Topic, utf8)},
    {ok, Producers} = rocketmq:ensure_supervised_producers(ClientId, ProducerGroup, Topic, ProducerOpts1),
    %% 载入生产者
    load_(Hook, {Filter, Producers, PayloadFormat}),
    [Producers | Acc] end, [], HookList).

%% 载入消息钩子处理函数
load_(Hook, Params) ->
  case Hook of
    'client.connected' ->
      emqx:hook(Hook, fun emqx_bridge_rocket:on_client_connected/3, [Params]);
    'client.disconnected' ->
      emqx:hook(Hook, fun emqx_bridge_rocket:on_client_disconnected/4, [Params]);
    'session.subscribed' ->
      emqx:hook(Hook, fun emqx_bridge_rocket:on_session_subscribed/4, [Params]);
    'session.unsubscribed' ->
      emqx:hook(Hook, fun emqx_bridge_rocket:on_session_unsubscribed/4, [Params]);
    'message.publish' ->
      emqx:hook(Hook, fun emqx_bridge_rocket:on_message_publish/2, [Params]);
    'message.acked' -> emqx:hook(Hook, fun emqx_bridge_rocket:on_message_acked/3, [Params]);
    'message.delivered' ->
      emqx:hook(Hook, fun emqx_bridge_rocket:on_message_delivered/3, [Params])
  end.

%% 卸载函数
unload() ->
  HookList = parse_hook(application:get_env(emqx_bridge_rocket, hooks,[])),
  lists:foreach(fun ({Hook, _, _, _}) -> unload_(Hook) end,HookList),
  io:format("~s is unloaded.~n", [emqx_bridge_rocket]),
  ok.

unload_(Hook) ->
  case Hook of
    'client.connected' -> emqx:unhook(Hook, fun emqx_bridge_rocket:on_client_connected/3);
    'client.disconnected' -> emqx:unhook(Hook, fun emqx_bridge_rocket:on_client_disconnected/4);
    'session.subscribed' -> emqx:unhook(Hook, fun emqx_bridge_rocket:on_session_subscribed/4);
    'session.unsubscribed' -> emqx:unhook(Hook, fun emqx_bridge_rocket:on_session_unsubscribed/3);
    'message.publish' -> emqx:unhook(Hook, fun emqx_bridge_rocket:on_message_publish/2);
    'message.acked' -> emqx:unhook(Hook, fun emqx_bridge_rocket:on_message_acked/3);
    'message.delivered' -> emqx:unhook(Hook, fun emqx_bridge_rocket:on_message_delivered/3)
  end.

%% 钩子处理客户端连接函数
on_client_connected(ClientInfo, _ConnInfo, {_, Producers, _}) ->
%%  增加客户端连接
  emqx_metrics:inc('bridge.rocket.client_connected'),
  %% 获取客户id
  ClientId = maps:get(clientid, ClientInfo, undefined),
  %% 获取用户名称
  Username = maps:get(username, ClientInfo, undefined),
  %% 封装消息
  Data = [
    {clientid, ClientId}, %% 客户ID
    {username, Username},%% 用户名
    {node, a2b(node())},%% erlang 结点
    {ts, erlang:system_time(millisecond)}],%% 连接时间
  %% 发送消息到rocket mq 中
  msg_to_rocket(Producers, Data),
  ok.

%% 处理断开连接消息
on_client_disconnected(ClientInfo, {shutdown, Reason}, ConnInfo, Envs) when is_atom(Reason); is_integer(Reason) ->
  on_client_disconnected(ClientInfo, Reason, ConnInfo, Envs);
on_client_disconnected(ClientInfo, Reason, _ConnInfo,{_, Producers, _}) when is_atom(Reason); is_integer(Reason) ->
  %% 断开连接
  emqx_metrics:inc('bridge.rocket.client_disconnected'),
  %%  从ClientInfo获取用户id
  ClientId = maps:get(clientid, ClientInfo, undefined),
%%  从ClientInfo 获取用户名称
  Username = maps:get(username, ClientInfo, undefined),
%%  封装数据
  Data = [{clientid, ClientId},
    {username, Username},
    {node, a2b(node())},
    {reason, a2b(Reason)},
    {ts, erlang:system_time(millisecond)}],
%%  发送消息到rocketmq
  msg_to_rocket(Producers, Data),
  ok;

%% 断开连接
on_client_disconnected(_ClientInfo, Reason, _ConnInfo, _Envs) ->
  logger:error("Client disconnected reason:~p not encode "
  "json",
    [Reason]),
  ok.

%% 消息订阅
on_session_subscribed(#{clientid := ClientId}, Topic, Opts, {Filter, Producers, _}) ->
%%  匹配消息主题
  case emqx_topic:match(Topic, Filter) of
%%    返回ok
    true ->
%%
      emqx_metrics:inc('bridge.rocket.session_subscribed'),
%%      格式化消息
      Data = format_sub_json(ClientId, Topic, Opts),
%%      发送消息到rocketmq
      msg_to_rocket(Producers, Data);
%%    匹配不到
    false -> ok
  end,
  ok.

%% 取消订阅
on_session_unsubscribed(#{clientid := ClientId}, Topic,Opts, {Filter, Producers, _}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      emqx_metrics:inc('bridge.rocket.session_unsubscribed'),
      Data = format_sub_json(ClientId, Topic, Opts),
      msg_to_rocket(Producers, Data);
    false -> ok
  end,
  ok.
%% 订阅消息发布
on_message_publish(Msg = #message{topic = Topic},{Filter, Producers, PayloadFormat}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      emqx_metrics:inc('bridge.rocket.message_publish'),
      Data = format_pub_msg(Msg, PayloadFormat),
      msg_to_rocket(Producers, Data);
    false -> ok
  end,
  {ok, Msg}.

on_message_acked(ClientInfo, Msg = #message{topic = Topic}, {Filter, Producers, PayloadFormat}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      emqx_metrics:inc('bridge.rocket.message_acked'),
      ClientId = maps:get(clientid, ClientInfo, undefined),
      Username = maps:get(username, ClientInfo, undefined),
      Data = format_revc_msg(ClientId,
        Username,
        Msg,
        PayloadFormat),
      msg_to_rocket(Producers, Data);
    false -> ok
  end,
  {ok, Msg}.

%% 订阅消息传递
on_message_delivered(ClientInfo, Msg = #message{topic = Topic}, {Filter, Producers, PayloadFormat}) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      emqx_metrics:inc('bridge.rocket.message_delivered'),
      ClientId = maps:get(clientid, ClientInfo, undefined),
      Username = maps:get(username, ClientInfo, undefined),
      Data = format_revc_msg(ClientId, Username, Msg, PayloadFormat),
      msg_to_rocket(Producers, Data);
    false -> ok
  end,
  {ok, Msg}.

parse_hook(Hooks) -> parse_hook(Hooks, []).

parse_hook([], Acc) -> Acc;
parse_hook([{Hook, Item} | Hooks], Acc) ->
  Params = emqx_json:decode(Item),
  Topic = get_value(<<"topic">>, Params),
  Filter = get_value(<<"filter">>, Params),
  PayloadFormat = application:get_env(emqx_bridge_rocket, encode_payload_type, base64),
  parse_hook(Hooks, [{l2a(Hook), Filter, Topic, PayloadFormat} | Acc]).

msg_to_rocket(Producers, Data) ->
  try produce(Producers, emqx_json:encode(Data)) catch
    Error:Reason:Stask ->
      logger:error("Call produce error: ~p, ~p", [Error, {Reason, Stask}])
  end.

produce(Producers, JsonMsg) when is_list(JsonMsg) -> produce(Producers, iolist_to_binary(JsonMsg));
produce(Producers = #{topic := Topic}, JsonMsg) ->
  logger:debug("Rocketmq produce topic:~p payload:~p", [Topic, JsonMsg]),
  case application:get_env(emqx_bridge_rocket, produce, sync) of
    sync ->
      Timeout = application:get_env(emqx_bridge_rocket, produce_sync_timeout, 3000),
      rocketmq:send_sync(Producers, JsonMsg, Timeout);
    async -> rocketmq:send(Producers, JsonMsg)
  end.

rocket_callback(_, _) -> ok.

format_sub_json(ClientId, Topic, Opts) ->
  Qos = maps:get(qos, Opts, 0),
  [{clientid, ClientId}, {topic, Topic}, {qos, Qos}, {node, a2b(node())}, {ts, erlang:system_time(millisecond)}].

format_pub_msg(Msg, PayloadFormat) ->
  #message{from = From, topic = Topic, payload = Payload, headers = Headers, qos = Qos, timestamp = Ts} = Msg,
  Username = maps:get(username, Headers, <<>>),
  [{clientid, From}, {username, Username}, {topic, Topic}, {payload, payload_format(Payload, PayloadFormat)}, {qos, Qos}, {node, a2b(node())}, {ts, Ts}].

format_revc_msg(ClientId, Username, Msg, PayloadFormat) ->
  #message{from = From, topic = Topic, payload = Payload, qos = Qos, timestamp = Ts} = Msg,
  [{clientid, ClientId}, {username, Username}, {from, From}, {topic, Topic},
    {payload, payload_format(Payload, PayloadFormat)}, {qos, Qos}, {node, a2b(node())}, {ts, Ts}].

payload_format(Payload, base64) ->
  base64:encode(Payload);
payload_format(Payload, _) -> Payload.

l2a(L) -> erlang:list_to_atom(L).

a2b(A) -> erlang:atom_to_binary(A, utf8).


