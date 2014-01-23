
-define(TXN_LOG_FLAG,"txnl").

-define(LONG_MAX,16#ffffffffffffffff).
-define(MAGIC_NUM,<<"SDFS">>).
-define(SIZE_OF_MAGIC_NUM,size(?MAGIC_NUM)).
-define(UNDEFINED_ZXID,0).


-ifdef(TEST).
-define(DEFAULT_LOAD_FILE_CHUNK_SIZE,512).
-define(ALLOC_SIZE_PER_PAGE,1024). %% default 32m per page.
-else.
-define(DEFAULT_LOAD_FILE_CHUNK_SIZE,1024*1024).
-define(ALLOC_SIZE_PER_PAGE,64*1024*1024). %% default 40m per page
-endif.

-define(FILE_WRITE_OPEN_OPTIONS,[write,read,raw,binary]).

-define(SIZE_OF_DATA_HEADER,28).

