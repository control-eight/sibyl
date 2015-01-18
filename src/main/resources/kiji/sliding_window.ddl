CREATE TABLE sliding_window WITH DESCRIPTION 'Sliding Window'
ROW KEY FORMAT RAW
WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = true,
  COMPRESSED WITH NONE,
  FAMILY info WITH DESCRIPTION 'basic information' (
    transaction CLASS com.my.sibyl.itemsets.kiji.Transaction WITH DESCRIPTION 'Transaction with items'
  )
);