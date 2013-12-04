
-define(APP_NAME,zab).
-define(ZAB_TYPE,skyfs_zab).

%% server state
-define(SERVER_STATE_LOOKING,0).
-define(SERVER_STATE_FOLLOWING,1).
-define(SERVER_STATE_LEADING,2).
-define(SERVER_STATE_OBSERVING,3).

-define(ELECTION_STATE_PREPARE,0).
-define(ELECTION_STATE_LOOKING,1).
-define(ELECTION_STATE_FLLOWER,2).
-define(ELECTION_STATE_LEADER,3).

-define(ELECTION_STATE_NAME_LOOKING,looking).
-define(ELECTION_STATE_NAME_FOLLOWING,following).
-define(ELECTION_STATE_NAME_LEADERING,leading).

%% election mod
%% -define(DEFAULT_ELECTION_MOD,fast_leader_election).

-define(DEFAULT_ELECTION_MOD,fast_leader_election_impl).

-ifdef('TEST').
-define(DEFAULT_INTERVAL_TIMEROUT,timer:seconds(2)).
-define(DEFAULT_ELECTION_TIMEOUT,timer:seconds(1)).
-define(DEFAULT_WAITING_TIMEOUT,timer:seconds(2)).
-else.
-define(DEFAULT_INTERVAL_TIMEROUT,timer:seconds(2)).
-define(DEFAULT_ELECTION_TIMEOUT,timer:seconds(5)).
-define(DEFAULT_WAITING_TIMEOUT,timer:seconds(8)).
-endif.

-ifdef('TEST').
-define(MAX_MSG_QUEUE_LEN,1000).
-define(TIME_OUT_WRITE,timer:seconds(5)).
-define(DEFAULT_TIMEOUT_SYNC,timer:seconds(2)).
-define(DEFAULT_TIMEOUT_INIT,timer:seconds(3)).
-define(DEFAULT_TIMEOUT_RECOVER,timer:seconds(20)).
-else.
-define(MAX_MSG_QUEUE_LEN,1000).
-define(TIME_OUT_WRITE,timer:seconds(15)).
-define(DEFAULT_TIMEOUT_SYNC,timer:seconds(2)).
-define(DEFAULT_TIMEOUT_INIT,timer:seconds(5)).
-define(DEFAULT_TIMEOUT_RECOVER,timer:seconds(20)).
-endif.


-ifdef('TEST').
-define(MAX_APPLY_LEN,2).
-define(COMMIT_ZXID_SIZE_PER_PAGE,32).
-else.
-define(MAX_APPLY_LEN,200).
-define(COMMIT_ZXID_SIZE_PER_PAGE,32*1024).
-endif.

-define(ZAB_STATE_RECOVERING,recoving).
-define(ZAB_STATE_SYNCING,syncing).
-define(ZAB_STATE_BROADCAST,broadcast).


-define(RECOVER_TYPE_DIFF,0).
-define(RECOVER_TYPE_SNAP,1).
-define(RECOVER_TYPE_TRUNC,2).

-define(RECOVER_LOAD_TXNLOG_PER_STEP,2000).


-define(ACK_TYPE_SYNC,0).
-define(ACK_BROADCAST,1).

-record(propose,{zxid,value}).
-record(follower_info,{id,last_zxid,last_commit_zxid,ref}).
-record(recover_response,{type,leader_last_zxid,data=[]}).
-record(ack,{type,id,zxid}).

-record(notification,{node,
				  	  epoch = -1,
				  	  zxid = -1,
				  	  proposed_leader,
				  	  proposed_epoch=-1,
				  	  proposed_zxid=-1,
					  ecount = 0
					 }).


-record(election,{node,
				  epoch = -1,
				  zxid = -1,
				  proposed_leader,
				  proposed_epoch=-1,
				  proposed_zxid=-1,
				  from,
				  timeout = ?DEFAULT_ELECTION_TIMEOUT,
				  timer,
				  votes = [],
				  zab_nodes = [],
				  ecount = 0,
				  quorum = 1,
				  state=?ELECTION_STATE_LOOKING,
				  followers = [],
				  followers_acks = []}).

-define(LAST_COMMIT_ZXID_FILE_NAME,"last_commit_zxid.log").