CREATE 'kiji.itemsets.table.sliding_window', {
    NAME => 'B',
    ENCODE_ON_DISK => 'true',
    BLOOMFILTER => 'NONE',
    VERSIONS => '1',
    IN_MEMORY => 'true',
    KEEP_DELETED_CELLS => 'false',
    DATA_BLOCK_ENCODING => 'NONE',
    TTL => '2147483647',
    COMPRESSION => 'NONE',
    MIN_VERSIONS => '0',
    BLOCKCACHE => 'true',
    BLOCKSIZE => '65536',
    REPLICATION_SCOPE => '0'
  },
  {SPLITS => ['a', 'b', 'c', 'd', 'e']}