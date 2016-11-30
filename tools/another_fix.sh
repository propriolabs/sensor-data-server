curl -XPUT "http://localhost:9200/proprio/_settings" -d '{ "index" : { "max_result_window" : 5000000 } }'
