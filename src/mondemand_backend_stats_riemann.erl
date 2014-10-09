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
           format_stat/8,
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
  Number = proplists:get_value (number, Config, 16), % FIXME: replace default
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

format_stat (_Prefix, ProgId, Host,
             _MetricType, MetricName, MetricValue, Timestamp, Context) ->
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
  }.

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
  case riemann:send (lists:flatten (Data)) of
    ok -> ok;
    {error, E} ->
      error_logger:error_msg ("Error sending to riemann : ~p",[E]),
      error
  end.

destroy (_) ->
  ok.
