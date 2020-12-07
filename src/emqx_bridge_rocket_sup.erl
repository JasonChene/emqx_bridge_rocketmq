%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:04
%%%-------------------------------------------------------------------
-module(emqx_bridge_rocket_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-vsn("4.2.1").

start_link() ->
  supervisor:start_link({local, emqx_bridge_rocket_sup}, emqx_bridge_rocket_sup, []).

init([]) -> {ok, {{one_for_one, 10, 100}, []}}.
