./datadiff \
  -interval 10000 \
  -mdriver 'es7' \
  -mconn 'http://localhost:9200' \
  -mconf '{"index":"i1", "field":"id"}' \
  -sdriver 'es7' \
  -sconn 'http://localhost:9200' \
  -sconf '{"index":"i2", "field":"id"}'
