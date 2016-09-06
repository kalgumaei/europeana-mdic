#!/usr/bin/python3
# -*- coding: utf-8 -*-
# europeana\metadataic\data_transform.py

import os
import sys
import codecs
import json
import gzip
import logging
import re
import socket

from pyspark import SparkContext
import elasticsearch
from elasticsearch import helpers

import conf
import index_settings
import ic_scoring

ES_HOSTS = conf.es_hosts
ES_PORT = conf.es_port
TARGET_FIELDS = conf.target_fields
DEFAULT_LANGUAGE = conf.default_language
DATA_PATH = conf.data_path
INDEX_NAME = conf.index_name
TYPE_NAME = conf.type_name
invalid_lang_flag = False
no_lang_flag = False
es = elasticsearch.Elasticsearch(hosts=ES_HOSTS.split(","), timeout=500)

def get_target_proxy(line_data):
    """Return the relative section of a metadata instnace."""
    try:
        for proxy in line_data["ore:Proxy"]:
            if proxy["edm:europeanaProxy"][0] == "false":
                return proxy
    except KeyError:
        return {}
    return {}


def get_field_data(line_data, field, source):
    """Extract a target filed data from a metadata instance."""

    target_proxy = get_target_proxy(line_data)
    if target_proxy:
        if field in target_proxy:
            return target_proxy[field]
        else:
            return []
    else:
        print(
            "'ore:Proxy' or 'edm:europeanaProxy' not found.\
              source[{s}] document[{d}]".format(
                  d=line_data['identifier'],
                  s=source))
    return []


def map_language(lan):
    """Map muliple forms language to one-form language code
       using language lookup table 'language_dict'."""
    language_dict = index_settings.language_dict
    if lan in language_dict:
        return (language_dict[lan])
    elif lan:
        return lan
    else:
        return DEFAULT_LANGUAGE


def find_language_in_proxy(proxy, doc, source):
    """Return the language at the level of the arget proxy."""
    lang = ""
    try:
        if len(proxy["dc:language"]) > 1:
            print(
                " 'dc:language' contains many elements {l}. \
                   source[{s}] document[{d}]".format(
                       l=proxy["dc:language"],
                       d=doc['identifier'],
                       s=source))
        if proxy["dc:language"][0]:
            if isinstance(proxy["dc:language"][0], dict):
                if "@lang" in proxy["dc:language"][0]:
                    lang = proxy["dc:language"][0]["@lang"]
                else:
                    print(
                        "Invalid nested fields in'dc:language' structure.\
                          source[{s}] document[{d}]".format(
                              d=doc["identifier"], s=source))
            elif isinstance(proxy["dc:language"][0], str):
                lang = proxy["dc:language"][0]
            else:
                print(
                    "Invalid 'dc:language' structure.\
                       source[{s}] document[{d}]".format(
                           d=doc['identifier'], s=source))
    except KeyError:
        lang = ""
    except IndexError:
        lang = ""
    if lang == "mul":
        lang = ""
    return lang


def find_language_out_proxy(doc):
    """Return the language at the higher level 'EuropeanaAggregation'."""
    lang = ""
    try:
        lang = doc["edm:EuropeanaAggregation"][0]["edm:language"][0]
    except KeyError:
        lang = ""
    except IndexError:
        lang = ""
    if lang == "mul":
        lang = ""
    return lang


def validate_lang_name(word):
    """Return True if language code does not includes whitespace
       or special char, otherwise, return False."""
    if re.match("^[\w]*$", word):
        return True
    else:
        return False


def extract_components(field_data, doc, source, field):
    """Return an array that contains simple elements in the
       form (text,lang) from a complex data field."""
    array = []
    for unit in field_data:
        if unit:
            # the field element has @lang and #value as nested elements
            if isinstance(unit, dict):
                if "@lang" in unit and "#value" in unit:
                    array.append([unit["#value"], unit["@lang"]])
                else:
                    print(
                        "Unknown nested fields'. source[{s}]\
                            document[{d}] field[{fl}]".format(
                                u=unit,
                                d=doc["identifier"],
                                s=source, fl=field))
            # only text
            elif isinstance(unit, str):
                array.append([unit, ""])
            else:
                print(
                    "Unknown field structure type. \
                     source[{s}] document[{d}] field[{fl}]".format(
                         u=unit,
                         d=doc["identifier"],
                         s=source, fl=field))
    return(array)


def add_nested_field_to_es(nested_field, text, es_doc):
    """ Add a text to an appropriate field in
        a given elasticsearch-form document."""
    if nested_field not in es_doc:
        # create a new
        es_doc[nested_field] = text
    else:
        # append to existed one
        es_doc[nested_field] = "|".join([es_doc[nested_field], text])
    return es_doc


def process_sub_field(element, back_language):
    """Return the element after setting the language."""
    global no_lang_flag
    global invalid_lang_flag

    if not element[1]:  # the element has no language
        if back_language:
            element[1] = back_language
        else:
            no_lang_flag = True
    if validate_lang_name(element[1]):
        final_lang = map_language(element[1].lower())
    else:
        invalid_lang_flag = True
        final_lang = DEFAULT_LANGUAGE
    return element[0], final_lang


def process_field(field, field_data, source, doc, back_language):
    """ Transform data fields into an elasticsearch-form document."""
    es_doc = {}
    elements = extract_components(field_data, doc, source, field)
    for element in elements:
        text, lang = process_sub_field(element, back_language)
        nested_field = "".join([field, "_", lang])
        es_doc = add_nested_field_to_es(nested_field, text, es_doc)
    return es_doc


def process_line(line_data, source):
    """Retuen a ready elasticsearch-form document for a metadta instance."""
    proxy = get_target_proxy(line_data)
    back_language = find_language_out_proxy(line_data)
    no_lang_flag = False
    invalid_lang_flag = False
    es_doc = {
        "doc_id": str(
            line_data["identifier"]),
        "doc_source": source}
    for field in TARGET_FIELDS:
        field_data = get_field_data(line_data, field, source)
        if field_data:
            result = process_field(
                field, field_data, source, line_data, back_language)
            if result:
                es_doc[field] = result

    if no_lang_flag:
        print(
            "No language is assigned. source[{s}] document[{d}]".format(
                d=line_data["identifier"], s=source))
    if invalid_lang_flag:
        print(
            " Invalid language code. source[{s}] document[{d}] ".format(
                d=line_data["identifier"], s=source))
    return(es_doc)


def transform_data(json_line):
    """Return the final prepared document to be indexed to elasticsearch."""
    spark_node = socket.gethostname()
    prepared_doc = process_line(json_line, spark_node)
    return prepared_doc


def parse_json(text_line):
    """Return the well-formed JSON lines, otherwise, retrun None."""
    try:
        return(json.loads(text_line))
    except ValueError:
        return(None)


def main(data_path=DATA_PATH, index_name=INDEX_NAME, type_name=TYPE_NAME):
    """Distribute data ransform and save the result to an Elasticsearch index."""
    print("Bulk indexing ... from ", data_path)
    lines = sc.textFile(data_path)
    sc.addFile("conf.py")
    sc.addFile("index_settings.py")
    es_documents = lines.map(parse_json).filter(
        lambda x: x).map(transform_data).map(
        lambda y: (
            "1", y))
    es_conf = {
        "es.nodes": ES_HOSTS,
        "es.port": ES_PORT,
        "es.resource": "/".join([index_name, type_name])}
    es_documents.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_conf)
    sc.stop()


if __name__ == "__main__":
    sc = SparkContext()
    if len(sys.argv) > 1:
        sys.exit(
            main(
                data_path=sys.argv[1],
                index_name=sys.argv[2],
                type_name=sys.argv[3]))
    else:
        print("Load default config ...")
        sys.exit(main())
