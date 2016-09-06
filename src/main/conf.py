#!/usr/bin/python3
# -*- coding: utf-8 -*-
# europeana\metadataic\conf.py

######## Application settings  ################
# data_path = "hdfs://es-spark1:9000/spark/europeana/*.gz"
#data_path = "hdfs://es-spark1:9000/spark/europeana/0_2020108_Ag_SI_Nat.EContentAggregator_Culture_si.json.gz"
data_path = "hdfs://es-spark1:9000/spark/europeana/1_09218_Ag_EU_EUscreen_National_Audiovisual_Archive.json.gz"
test_data_path = "hdfs://es-spark1:9000/spark/europeana/test/test_json_data.gz"
default_language = "unknown"
tfidf_formula = "term_freq * math.log(1 + (all_docs / doc_freq ))"
target_fields = ["dc:title", "dc:description", "dcterms:alternative"]


######### eslasticsearch settings  ############
es_hosts = "10.254.1.5,10.254.1.4,10.254.1.2"
es_port = "9200"
# Original index
index_name = 'europeana-55'
type_name = 'resources'
# IC measurment result index
ic_index_name = 'europeana-55-result'
ic_type_name = 'resources'

######### spark settings  ######################
spark_master = "es-spark2:7077"
es_shards_to_spark_partitions = 1200
