CREATE TABLE recommendations_gathering WITH DESCRIPTION 'Recommendations Gathering Helper'
ROW KEY FORMAT (itemset STRING)
WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = true,
  COMPRESSED WITH NONE,
  BLOOM FILTER = ROW,
  FAMILY info WITH DESCRIPTION 'basic information' (
    count "int" WITH DESCRIPTION 'Frequency'
  )
);