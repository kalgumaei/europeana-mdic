#!/usr/bin/python3
# -*- coding: utf-8 -*-
# europeana-mdic/src/main/conf.py

######### General settings
data_path = "hdfs://es-spark1:9000/spark/europeana/*.gz"
test_data_path = "hdfs://es-spark1:9000/spark/europeana/test/test_json_data.gz"
default_language = "unknown"
tfidf_formula = "term_freq * math.log(1 + (all_docs / doc_freq ))"
target_fields = ["dc:title", "dc:description", "dcterms:alternative"]

######### Elasticsearch settings
es_hosts = "10.254.1.5,10.254.1.4,10.254.1.2"
es_port = "9200"
# Original index
index_name = 'europeana-5m'
type_name = 'resources'
# Result index
ic_index_name = 'europeana-5m-result'
ic_type_name = 'resources'
 
######### spark settings
spark_master = "es-spark2:7077"
es_shards_to_spark_partitions = 500
