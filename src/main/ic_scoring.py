#!/usr/bin/python3
# -*- coding: utf-8 -*-
# europeana\metadataic\ic_scoring.py

import sys
import math
import time
import datetime
import logging
from distutils.util import strtobool

import elasticsearch
from elasticsearch import helpers
from pyspark import SparkContext

import conf
import index_settings
import data_transform

ES_HOSTS = conf.es_hosts
ES_PORT = conf.es_port
es = elasticsearch.Elasticsearch(hosts=ES_HOSTS.split(","), timeout=500)

INDEX_NAME = conf.index_name
TYPE_NAME = conf.type_name
IC_INDEX_NAME = conf.ic_index_name
IC_TYPE_NAME = conf.ic_type_name
TARGET_FIELDS = conf.target_fields
TFIDF_FORMULA = conf.tfidf_formula
PARTITIONS = conf.es_shards_to_spark_partitions

def get_input():
    """Get an input from keyboard."""
    return input()


def prompt(message):
    """Show a prompet and get only a correct answer."""
    print("{ms}[y/n]: ".format(ms=message))
    val = get_input()
    try:
        ret = strtobool(val)
    except ValueError:
        print("Please answer with y/Y or n/N")
        return prompt(message)
    return ret


def get_termvectors(index_name, doc_type, internal_id, field, doc_id, doc_src):
    """Return the termvector for the text of a given feild. """
    distr_es = elasticsearch.Elasticsearch(
        hosts=conf.es_hosts.split(","),
        port=conf.es_port,
        timeout=500)
    a = {}
    try:
        a = distr_es.termvectors(index=index_name,
                                 doc_type=doc_type,
                                 id=internal_id,
                                 dfs=True,
                                 offsets=False,
                                 payloads=False,
                                 positions=False,
                                 field_statistics=True,
                                 term_statistics=True,
                                 fields=[field]
                                 )
    except Exception as es1:
        print(
            "Cann't retrive termvector for doc[{d_id}],\
                           field[{f}] in source[{src}]".format(
                d_id=doc_id,
                f=field,
                src=doc_src
            )
        )
    return (a)


def calculate_tfidf(termvector, field):
    """Calculate TF-IDF for a given term vector."""
    tfidf = 0.0
    terms = termvector["term_vectors"][field]["terms"]
    length = len(terms)
    for token in terms:
        term_freq = terms[token]["term_freq"]
        doc_freq = terms[token]["doc_freq"]
        all_docs = termvector["term_vectors"][
            field]["field_statistics"]["doc_count"]
        tfidf = tfidf + eval(TFIDF_FORMULA)
    return tfidf, length


def get_full_name(parent, child):
    """Return full dotted path of a child field."""
    return ("".join([parent, ".", child]))


def score_subfield(
        doc,
        parent,
        child,
        field_tfidf,
        child_tfidf_list,
        info_density_list):
    """Return TF-IDF and information density of a child field."""
    tfidf = 0.0
    no_of_terms = 0
    tv = get_termvectors(
        doc["_index"],
        doc["_type"],
        doc["_id"],
        get_full_name(
            parent,
            child),
        doc["_source"]["doc_id"],
        doc["_source"]["doc_source"])
    field_tfidf[child] = {}
    if tv["term_vectors"]:
        tfidf, no_of_terms = calculate_tfidf(tv, get_full_name(parent, child))
    field_tfidf[child]["tfidf_score"] = tfidf
    field_tfidf[child]["terms_count"] = no_of_terms
    if no_of_terms:
        field_tfidf[child]["info_density"] = tfidf / no_of_terms
    else:
        field_tfidf[child]["info_density"] = 0
    child_tfidf_list.append(tfidf)
    info_density_list.append(field_tfidf[child]["info_density"])
    return field_tfidf, child_tfidf_list, info_density_list


def score_field(doc, field, prepared_doc):
    """Return TF-IDF and information density of a field and its
       children fields."""  
    field_tfidf = {}
    child_tfidf_list = []
    info_density_list = []
    for child in [element for element in doc["_source"][field]]:
        field_tfidf, child_tfidf_list, info_density_list = score_subfield(
            doc,
            field,
            child,
            field_tfidf,
            child_tfidf_list,
            info_density_list)
    if field_tfidf:
        prepared_doc["_source"][field] = {"sub_tfidf": field_tfidf}
        prepared_doc["_source"][field][
            "field_ic_score"] = sum(child_tfidf_list)
        prepared_doc["_source"][field][
            "field_info_density_score"] = sum(info_density_list)
        prepared_doc["_source"]["doc_ic_score"] = prepared_doc["_source"][
            "doc_ic_score"] + prepared_doc["_source"][field]["field_ic_score"]
        prepared_doc["_source"]["doc_info_density_score"] = \
            prepared_doc["_source"]["doc_info_density_score"] + \
            prepared_doc["_source"][field]["field_info_density_score"]
    return(prepared_doc)


def score_instance(doc):
    """Reurn all IC information for one metadata instance."""
    prepared_doc = {"_id": doc['_id'],
                    # initialize score for each document is 0.0
                    "_source": {"doc_id": doc['_source']["doc_id"],
                                "doc_source": doc['_source']["doc_source"],
                                "doc_ic_score": 0.0,
                                "doc_info_density_score": 0.0
                                }
                    }
    for field in [fields for fields in doc["_source"]
                  if fields in TARGET_FIELDS]:
        prepared_doc = score_field(doc, field, prepared_doc)

    if prepared_doc["_source"]["doc_ic_score"]:
        prepared_doc["_source"]["doc_ic_score"] = math.log(
            1 + prepared_doc["_source"]["doc_ic_score"])
    if prepared_doc["_source"]["doc_info_density_score"]:
        prepared_doc["_source"]["doc_info_density_score"] = math.log(
            1 + prepared_doc["_source"]["doc_info_density_score"])
    return prepared_doc

def get_index_name():
    """Return the original index name."""
    return(INDEX_NAME)


def scoring(es_doc):
    """Return the final scored instance as an elasticsearch-format document."""
    doc = {"_source": es_doc[1],
           "_id": es_doc[0],
           "_index": get_index_name(),
           "_type": TYPE_NAME
           }
    result = score_instance(doc)
    
    return (es_doc[0], result["_source"])


def create_index(i_name, t_name, my_setting, my_mapping):
    """create a new index and apply the mapping if found."""
    try:
        es.indices.create(index=i_name,
                          body=my_setting
                          )
        if my_mapping:
            es.indices.put_mapping(
                index=i_name,
                doc_type=t_name,
                body=my_mapping)
    except Exception:
        print("Unexpected error:", sys.exc_info()[0])
        return False
    return True


def init_index(index_name, type_name, my_setting, my_mapping):
    """Checks if the index exists and put some options questions."""
    if es.indices.exists(index=index_name):
        if prompt("The index already exists, delete it and create a new one?"):
            es.indices.delete(index=index_name)
            create_index(index_name, type_name, my_setting, my_mapping)
            return True

        else:
            if prompt(
                    "the data will be indexed (appended)\
                      to the existing index! Continue?"):
                return True
            else:
                return False
    else:
        create_index(index_name, type_name, my_setting, my_mapping)
        return True


def docs_count(index_name, doc_type):
    """Return number of documents in a given index."""
    try:
        es.indices.refresh(index=index_name)
        return(es.count(index=index_name, doc_type=doc_type)['count'])
    except Exception:
        print("Unexpected error:", sys.exc_info()[0])
        return None


def es_replication(index_name, replicas):
    """Edit number of index replicas."""
    try:
        es.indices.put_settings(index=index_name, body={
            "index": {
                "number_of_replicas": replicas,
                "refresh_interval": "1s"
            }
        })
        print(
            "{r} replicas are set for index= {i}".format(
                r=replicas,
                i=index_name))
    except Exception:
        print("Elasticsearch error:", sys.exc_info()[0])


def main():
    """Read metadata from an Elasticsearch index,calculate the IC in parallel,
       and save the result back to an IC result index."""
    sc.addFile("conf.py")
    sc.addFile("index_settings.py")
    sc.addFile("data_transform.py")
    es_read_conf = {
        "es.nodes": ES_HOSTS,
        "es.port": ES_PORT,
        "es.resource": "/".join([INDEX_NAME, TYPE_NAME])
    }
    # Read documents data from elasticsearch
    es_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_read_conf)
    es_rdd_repartitioned = es_rdd.repartition(PARTITIONS)
    scored_docs_rdd = es_rdd_repartitioned.map(scoring)
    es_write_conf = {
        "es.nodes": ES_HOSTS,
        "es.port": ES_PORT,
        "es.resource": "/".join([IC_INDEX_NAME, IC_TYPE_NAME])
    }
    # Save IC scoring results to elasticsearch
    scored_docs_rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)
    sc.stop()


if __name__ == '__main__':
    sc = SparkContext()
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(
        __name__)      # logging
    logger.warn("See this log message in the file /home/spuser/logfile.out")
    
    if len(sys.argv) > 1:
        INDEX_NAME = sys.argv[1]
        TYPE_NAME = sys.argv[2]
        IC_INDEX_NAME = sys.argv[3]
        IC_TYPE_NAME = sys.argv[4]
        PARTITIONS = int(sys.argv[5])
    print("index=",INDEX_NAME , "res_index=",IC_INDEX_NAME)
    sys.exit(main())

