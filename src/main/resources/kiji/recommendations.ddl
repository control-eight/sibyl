CREATE TABLE recommendations WITH DESCRIPTION 'Recommendations'
ROW KEY FORMAT (itemset STRING)
WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = true,
  COMPRESSED WITH NONE,
  BLOOM FILTER = ROW,
  FAMILY info WITH DESCRIPTION 'basic information' (
    recommendations CLASS com.my.sibyl.itemsets.kiji.RecommendedProducts WITH DESCRIPTION 'Recommended Product'
  )
);