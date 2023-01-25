-module(remote_logger).
 
-export([
    activate/1,
    activate/2,
    deactivate/2,
    remote_loader/1
]).
 
 
activate(Node) ->
    {group_leader, LocalGL} = process_info(whereis(logger_sup), group_leader),
    activate(Node, LocalGL).

activate(Node, LocalGL) ->
    {ok, OriginalGL} = rpc:call(
        Node, erlang, apply, [fun remote_loader/1, [LocalGL]]
    ),
    {ok, OriginalGL}.
 
 
deactivate(Node, OriginalGL) ->
    {ok, PrevGL} = rpc:call(Node, erlang, apply, [fun remote_loader/1, [OriginalGL]]),
    {ok, PrevGL}.
 
 
remote_loader(RemoteGL) ->
    Sup = whereis(logger_sup),
    {group_leader, OriginalGL} = process_info(Sup, group_leader),
    group_leader(RemoteGL, Sup),
    logger:reconfigure(),
    {ok, OriginalGL}.