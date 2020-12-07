%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:04
%%%-------------------------------------------------------------------
-module(emqx_bridge_rocket_app).
-behaviour(application).

-emqx_plugin(bridge).

-include("../include/emqx_bridge_rocket.hrl").


-export([start/2, stop/1, prep_stop/1]).

-vsn("4.2.1").

start(_Type, _Args) ->
  {ok, _} = application:ensure_all_started(rocketmq),

  ClientId = emqx_bridge_rocket,

%%  获取rockmq的服务配置
  Servers = application:get_env(emqx_bridge_rocket, servers, [{"localhost", 9876}]),
%%  启动
  {ok, _Pid} = rocketmq:ensure_supervised_client(ClientId, Servers, #{}),

%%  启动应用监听
  {ok, Sup} = emqx_bridge_rocket_sup:start_link(),
%%  启动测量
  emqx_bridge_rocket:register_metrics(),
%%  载入生产者
  NProducers = emqx_bridge_rocket:load(ClientId),
  {ok, Sup, #{client_id => ClientId, n_producers => NProducers}}.

prep_stop(State = #{client_id := ClientId, n_producers := NProducers}) ->
%%  卸载模块
  emqx_bridge_rocket:unload(),
%%  循环定制生产者
  lists:foreach(fun (Producers) -> rocketmq:stop_and_delete_supervised_producers(Producers) end, NProducers),
%%  停止和删除客户端监控者
  ok = rocketmq:stop_and_delete_supervised_client(ClientId),
  State.

stop(_) -> ok.