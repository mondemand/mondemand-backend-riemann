-module (mondemand_backend_stats_riemann).

-include_lib ("riemann/include/riemann_pb.hrl").

-behaviour (supervisor).
-behaviour (mondemand_server_backend).
-behaviour (mondemand_backend_stats_handler).
-behaviour (mondemand_backend_worker).

%% mondemand_backend_worker callbacks
-export ([ create/1,
           send/2,
           destroy/1 ]).

%% mondemand_backend callbacks
-export ([ start_link/1,
           process/1,
           required_apps/0,
           type/0
         ]).

%% mondemand_backend_stats_handler callbacks
-export ([ header/0,
           format_stat/10,
           separator/0,
           footer/0,
           handle_response/2
         ]).

%% supervisor callbacks
-export ([ init/1 ]).

-record (state, { }).

%%====================================================================
%% mondemand_server_backend callbacks
%%====================================================================
start_link (Config) ->
  supervisor:start_link ( { local, ?MODULE }, ?MODULE, [Config]).

process (Event) ->
  mondemand_backend_worker_pool_sup:process
    (mondemand_backend_stats_riemann_worker_pool, Event).

required_apps () ->
  [ compiler, syntax_tools, lager, riemann ].

type () ->
  supervisor.

%%====================================================================
%% supervisor callbacks
%%====================================================================
init ([Config]) ->
  % default to one process per scheduler
  Number = proplists:get_value (number, Config, erlang:system_info(schedulers)),

  { ok,
    {
      {one_for_one, 10, 10},
      [
        { mondemand_backend_stats_riemann_worker_pool,
          { mondemand_backend_worker_pool_sup, start_link,
            [ mondemand_backend_stats_riemann_worker_pool,
              mondemand_backend_worker,
              Number,
              ?MODULE ]
          },
          permanent,
          2000,
          supervisor,
          [ ]
        }
      ]
    }
  }.

%%====================================================================
%% mondemand_backend_stats_handler callbacks
%%====================================================================
header () -> [].

separator () -> [].

format_stat (_Num, _Total, _Prefix, ProgId, Host,
             MetricType, MetricName, MetricValue, Timestamp, Context) ->
  case MetricType of
    statset ->
      lists:map (
        fun ({SubType, SubTypeValue}) ->
          #riemannevent {
            service = ProgId,
            state = "ok",
            description =
              MetricName ++ "_" ++ mondemand_util:stringify (SubType),
            metric_sint64 = SubTypeValue,
            metric_f = SubTypeValue * 1.0,
            time = Timestamp,
            host = Host,
            attributes =
              [ #riemannattribute { key = CK, value = CV }
                || { CK, CV} <- Context ]
          }
        end,
        mondemand_statsmsg:statset_to_list (MetricValue)
      );
    _ ->
      #riemannevent {
        service = ProgId,
        state = "ok",
        description = MetricName,
        metric_sint64 = MetricValue,
        metric_f = MetricValue * 1.0,
        time = Timestamp,
        host = Host,
        attributes =
          [ #riemannattribute { key = CK, value = CV }
            || { CK, CV} <- Context ]
      }
  end.

footer () -> [].

handle_response (Response, _Previous) ->
  error_logger:info_msg ("~p : got unexpected response ~p",[?MODULE, Response]),
  { 0, undefined }.


%%====================================================================
%% mondemand_backend_worker callbacks
%%====================================================================
create (_) ->
  #state {}.

send (_, Data) ->
  case lists:flatten(Data) of
    [] ->
      error_logger:error_msg ("No data to send to Riemann"),
      error;
    NewData ->
      try riemann:send (NewData) of
        ok -> ok;
        {error, E} ->
          error_logger:error_msg ("Error sending to riemann : ~p",[E]),
          error
      catch
        E1:E2 ->
          error_logger:error_msg ("Error sending to riemann : ~p:~p",[E1,E2]),
          error
      end
  end.


destroy (_) ->
  ok.
