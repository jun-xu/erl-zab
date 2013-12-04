
-define(LONG_MAX,16#ffffffffffffffff).
-define(MAGIC_NUM,<<"SDFS">>).
-define(SIZE_OF_MAGIC_NUM,size(?MAGIC_NUM)).
-define(UNDEFINED_ZXID,0).

-ifdef(TEST).
-define(ALLOC_SIZE_PER_PAGE,64*1024). %% default 32m per page.
-define(INDEX_ALLOC_SIZE_PER_PAGE,64).
-define(INDEX_START_OFFSET,?ALLOC_SIZE_PER_PAGE - ?INDEX_ALLOC_SIZE_PER_PAGE).
-else.
-define(ALLOC_SIZE_PER_PAGE,80*1024*1024). %% default 80m per page,512k for index, another for data.
-define(INDEX_ALLOC_SIZE_PER_PAGE,32*1024*16). %% assume 128byte per msg,
-define(INDEX_START_OFFSET,?ALLOC_SIZE_PER_PAGE - ?INDEX_ALLOC_SIZE_PER_PAGE).
-endif.

-define(FILE_WRITE_OPEN_OPTIONS,[write,read,raw,binary]).

-define(SIZE_OF_INDEX_BINARY,32).
-define(SIZE_OF_DATA_HEADER,24).

-record(log_index,{zxid,
			   	   offset,
				   length
			}).

-record(log_data,{crc32,		%%check sum of data.
				  length,		%%length of data.
				  time,
				  value}).		%%binary data.


