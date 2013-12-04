##erl-zab##
pure zab protocol implement by erlang.


##configuration##
zab/etc/app.config:

	known_nodes:集群所有节点名
	apply_mod:  应用模块接口，格式{Module,Function}
	配置好cookie后用erl-zab/bin/erl-zab.sh console启动节点


##API##


- **1 客户端写消息**
	
	zab:write(Msg::term()) ->{ok,Zxid} | {error,Reason}



- **2 获取节点目前状态**
	
	zab:get_state() ->{ok,{State,FollowerList,Leader}}
	
	%% server state:


	0  正在查找leader.
	1  找到leader，自身处于跟随.
	2  自身是leader.
	3  观察(暂未使用).

- **3 获取节点最新同步的id**
	
	zab:get_last_zxid() -> {ok,Zxid} | {error,Reason}
	

- **4 获取最新应用的id**
   
	zab:get_last_commit_zxid() ->{ok,Zxid} | {error,Reason}
    

- **5 获取消息**

	zab:get_value(Zxid) -> {ok,Value} | {error,Reason}
    
	配置文件默认apply_mod为{arithmetic,call},这是一个处理四则运算的模块(用于测试)
	处理{plus,X,Y}
		{minus,X,Y}
		{times,X,Y}
		{divide,X,Y}
		
		
		
