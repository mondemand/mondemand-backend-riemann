-module (mondemand_backend_stats_riemann).

-include_lib ("lwes/include/lwes.hrl").
-include_lib ("riemann/include/riemann_pb.hrl").

-behaviour (mondemand_server_backend).
-behaviour (gen_server).

%% mondemand_backend callbacks
-export ([ start_link/1,
           process/1,
           stats/0,
           required_apps/0
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, { stats }).

%%====================================================================
%% API
%%====================================================================
start_link (Config) ->
  gen_server:start_link ( { local, ?MODULE }, ?MODULE, Config, []).

process (Event) ->
  gen_server:cast (?MODULE, {process, Event}).

stats () ->
  gen_server:call (?MODULE, {stats}).

required_apps () ->
  [ compiler, syntax_tools, lager, riemann ].

%%====================================================================
%% gen_server callbacks
%%====================================================================
init (_Config) ->
  InitialStats =
    mondemand_server_util:initialize_stats ([ errors, processed ] ),
  { ok, #state { stats = InitialStats } }.

handle_call ({stats}, _From,
             State = #state { stats = Stats }) ->
  { reply, Stats, State };
handle_call (Request, From, State) ->
  error_logger:warning_msg ("~p : Unrecognized call ~p from ~p~n",
                            [?MODULE, Request, From]),
  { reply, ok, State }.

handle_cast ({process, Binary},
             State = #state { stats = Stats }) ->
  Event =  lwes_event:from_udp_packet (Binary, dict),
  #lwes_event { attrs = Data } = Event,

  Timestamp = dict:fetch (<<"ReceiptTime">>, Data),

  Num = dict:fetch (<<"num">>, Data),
  ProgId = dict:fetch (<<"prog_id">>, Data),
  { Host, Context } =
    case mondemand_server_util:construct_context (Event) of
      [] -> { "unknown", [] };
      C ->
        case lists:keytake (<<"host">>, 1, C) of
          false -> { "unknown", C };
          {value, {<<"host">>, H}, OC } -> {H, OC}
        end
    end,

  Events =
    lists:map (
      fun(E) ->
        K = dict:fetch (mondemand_server_util:stat_key (E), Data),
        V = dict:fetch (mondemand_server_util:stat_val (E), Data),
        #riemannevent {
          service = ProgId,
          state = "ok",
          description = K,
          metric_sint64 = V,
          metric_f = V * 1.0,
          time = Timestamp,
          host = Host,
          attributes =
            [ #riemannattribute { key = CK, value = CV }
              || { CK, CV} <- Context ]
        }
      end,
      lists:seq (1,Num)
    ),
  riemann:send (Events),
  { noreply,
    State#state {
      stats = mondemand_server_util:increment_stat (processed, Num, Stats)
    }
  };

handle_cast (Request, State) ->
  error_logger:warning_msg ("~p : Unrecognized cast ~p~n",[?MODULE, Request]),
  { noreply, State }.

handle_info (Request, State) ->
  error_logger:warning_msg ("~p : Unrecognized info ~p~n",[?MODULE, Request]),
  {noreply, State}.

terminate (_Reason, #state { }) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.
