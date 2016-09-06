#!/usr/bin/python3
# -*- coding: utf-8 -*-
# metadataic\test\test_data_transform.py

import subprocess
import shlex
import socket
import unittest
from unittest.mock import patch

import elasticsearch
from pyspark import SparkConf, SparkContext

import data_transform
import index_settings
import conf


class DataTransformTestCase(unittest.TestCase):

    def test_get_target_proxy(self):
        doc_ok = {"a": "test",
                  "ore:Proxy": [{"x": "yes",
                                 "edm:europeanaProxy": ['true'],
                                 "dc:title": ["yes yes"]},
                                {"x": "no",
                                 "edm:europeanaProxy": ['false'],
                                 "dc:title": ["no no"]}]}
        doc_double_false_proxy = {"a": "test",
                                  "ore:Proxy":
                                  [{"x": "yes",
                                    "edm:europeanaProxy": ['false'],
                                    "dc:title": ["yes yes"]},
                                   {"x": "no",
                                    "edm:europeanaProxy": ['false'],
                                    "dc:title": ["no no"]
                                    }
                                   ]
                                  }
        doc_no_proxy = {"a": "test", "b": 222}
        doc_no_europeanaProxy = {"a": "test", "ore:Proxy": [
            {"x": "some text", "dc:title": ["test title"]}]}
        doc_no_europeanaProxyFalse = {"a": "test", "ore:Proxy": [
            {"x": "some text",
             "edm:europeanaProxy": ['true'],
             "dc:title": ["test title"]}]}

        self.assertEqual(data_transform.get_target_proxy(doc_ok),
                         {"x": "no",
                          "edm:europeanaProxy": ['false'],
                          "dc:title": ["no no"]},
                         msg="Error in [normal doc] case")

        self.assertEqual(
            data_transform.get_target_proxy(doc_double_false_proxy),
            {
                "x": "yes",
                "edm:europeanaProxy": ['false'],
                "dc:title": ["yes yes"]},
            msg="Error in [double false proxy")

        self.assertEqual(data_transform.get_target_proxy(doc_no_proxy), {},
                         msg="Error in [no 'ore:Proxy' field] case")

        self.assertEqual(data_transform.get_target_proxy(
            doc_no_europeanaProxy),
                         {},
                         msg="Error in [no 'edm:europeanaProxy' field] case")

        self.assertEqual(data_transform.get_target_proxy(
            doc_no_europeanaProxyFalse),
                         {},
                         msg="Error in ['edm:europeanaProxy' <> false] case")

    def test_get_field_data(self):
        line_data_simple = {"a": "test", "identifier": '7777', "ore:Proxy": [
            {"x": "yes",
             "edm:europeanaProxy": ['false'],
             "dc:title": ["yes yes"]}]}
        line_data_multiple = {"a": "test",
                              "identifier": '7777',
                              "ore:Proxy": [{"x": "yes",
                                             "edm:europeanaProxy": ['false'],
                                             "dc:title": ["yes1",
                                                          "yes2",
                                                          "yes3"]
                                             }
                                            ]
                              }
        line_data_struct = {"a": "test",
                            "identifier": '7777',
                            "ore:Proxy": [{"x": "yes",
                                           "edm:europeanaProxy": ['false'],
                                           "dc:title": [{"@lang": "en",
                                                         "#value": "yes4"}]}]}
        line_data_no_field = {"a": "test", "identifier": '7777', "ore:Proxy": [
            {"x": "yes", "edm:europeanaProxy": ['false']}]}

        self.assertEqual(
            data_transform.get_field_data(
                line_data_simple,
                "dc:title",
                "any_file_name"),
            ["yes yes"],
            msg="Error in [line_data_simple] case")

        self.assertEqual(
            data_transform.get_field_data(
                line_data_multiple,
                "dc:title",
                "any_file_name"),
            ["yes1", "yes2", "yes3"],
            msg="Error in [line_data_multiple] case")

        self.assertEqual(data_transform.get_field_data(line_data_struct,
                                                       "dc:title",
                                                       "any_file_name"),
                         [{"@lang": "en", "#value": "yes4"}],
                         msg="Error in [line_data_struct] case")

        self.assertEqual(
            data_transform.get_field_data(
                line_data_no_field,
                "dc:title",
                "any_file_name"),
            [],
            msg="Error in [line_data_no_field] case")

    def test_map_language(self):
        self.assertEqual(
            data_transform.map_language("en"),
            "english",
            msg="Error in [found in the languages dict] case")
        self.assertEqual(
            data_transform.map_language("WXYZ"),
            "WXYZ",
            msg="Error in [not found in the languages dict] case")
        self.assertEqual(
            data_transform.map_language(""),
            conf.default_language,
            msg="Error in [language is NULL] case")

    def test_find_language_in_proxy(self):
        proxy_lang_ok = {
            "x": "yes",
            "edm:europeanaProxy": ['true'],
            "dc:title": ["yes yes"],
            "dc:language": ["en"]}
        proxy_lang_array = {
            "x": "yes",
            "edm:europeanaProxy": ['true'],
            "dc:title": ["yes yes"],
            "dc:language": [
                "fr",
                "en"]}
        proxy_lang_struct_ok = {"x": "yes",
                                "edm:europeanaProxy": ['true'],
                                "dc:title": ["yes yes"],
                                "dc:language": [{"@lang": "it"}, "fr"]
                                }
        proxy_lang_struct_wrong = {"x": "yes",
                                   "edm:europeanaProxy": ['true'],
                                   "dc:title": ["yes yes"],
                                   "dc:language": [{"@other": "en"}, "fr"]
                                   }
        proxy_lang_null = {
            "x": "yes",
            "edm:europeanaProxy": ['true'],
            "dc:title": ["yes yes"],
            "dc:language": []}
        proxy_lang_mul = {
            "x": "yes",
            "edm:europeanaProxy": ['true'],
            "dc:title": ["yes yes"],
            "dc:language": ["mul"]}

        self.assertEqual(
            data_transform.find_language_in_proxy(
                proxy_lang_ok,
                {'identifier': "7777"},
                "any_file_name"), "en",
            msg="Error in [proxy_lang_ok] case")
        self.assertEqual(
            data_transform.find_language_in_proxy(
                proxy_lang_array,
                {'identifier': "7777"},
                "any_file_name"), "fr",
            msg="Error in [proxy_lang_array] case")
        self.assertEqual(
            data_transform.find_language_in_proxy(
                proxy_lang_struct_ok,
                {'identifier': "7777"},
                "any_file_name"), "it",
            msg="Error in [proxy_lang_struct_ok] case")
        self.assertEqual(
            data_transform.find_language_in_proxy(
                proxy_lang_struct_wrong,
                {'identifier': "7777"},
                "any_file_name"), "",
            msg="Error in [proxy_lang_struct_wrong] case")
        self.assertEqual(
            data_transform.find_language_in_proxy(
                proxy_lang_null,
                {'identifier': "7777"},
                "any_file_name"), "",
            msg="Error in [proxy_lang_null] case")
        self.assertEqual(
            data_transform.find_language_in_proxy(
                proxy_lang_mul,
                {'identifier': "7777"},
                "any_file_name"), "",
            msg="Error in [proxy_lang_mul] case")

    def test_find_language_out_proxy(self):
        doc_language_ok = {"a": "test",
                           "ore:Proxy": [{"a": 50}],
                           "edm:EuropeanaAggregation": [
                               {"edm:language": ["en"]}
                               ]
                           }
        doc_no_edm_language = {"a": "test",
                               "ore:Proxy": [{"a": 50}],
                               "edm:EuropeanaAggregation": [
                                   {"x": ["anything"]}
                                   ]
                               }
        doc_mul_language = {"a": "test",
                            "ore:Proxy": [{"a": 50}],
                            "edm:EuropeanaAggregation": [
                                {"edm:language": ["mul"]}
                                ]
                            }
        doc_language_null = {"a": "test",
                             "ore:Proxy": [{"a": 50}],
                             "edm:EuropeanaAggregation": [
                                 {"edm:language": []}
                                 ]
                             }
        doc_language_blank = {"a": "test",
                              "ore:Proxy": [{"a": 50}],
                              "edm:EuropeanaAggregation": [
                                  {"edm:language": [""]}
                                  ]
                              }
        self.assertEqual(data_transform.find_language_out_proxy(
            doc_language_ok), "en",
                         msg="Error in [doc_ok] case")
        self.assertEqual(data_transform.find_language_out_proxy(
            doc_no_edm_language), "",
                         msg="Error in [doc_no_edm_language] case")
        self.assertEqual(data_transform.find_language_out_proxy(
            doc_mul_language), "",
                         msg="Error in [doc_mul_language] case")
        self.assertEqual(data_transform.find_language_out_proxy(
            doc_language_null), "",
                         msg="Error in [doc_language_null] case")
        self.assertEqual(data_transform.find_language_out_proxy(
            doc_language_blank), "",
                         msg="Error in [doc_language_blank] case")

    def test_validate_lang_name(self):
        self.assertTrue(
            data_transform.validate_lang_name("ENG"),
            msg="Error in [valid lang name only characters] case")
        self.assertTrue(
            data_transform.validate_lang_name("german2"),
            msg="Error in [valid lang name only characters char+Num] case")
        self.assertTrue(
            data_transform.validate_lang_name("german_2"),
            msg="Error in [valid lang name with _] case")
        self.assertFalse(
            data_transform.validate_lang_name("german$"),
            msg="Error in [invalid lang name with spic. char] case")
        self.assertTrue(
            data_transform.validate_lang_name("görman"),
            msg="Error in [invalid lang name with non-ascii ö] case")
        self.assertFalse(
            data_transform.validate_lang_name("eng lish"),
            msg="Error in [lang name with space] case")

    def test_extract_component(self):
        field_data_struct_normal = [{"@lang": "en", "#value": "title text"}, {
            "@lang": "ar", "#value": "نص العنوان"}]
        field_data_struct_one_wrong_key = [{"@other": "en",
                                            "#value": "title text"
                                            },
                                           {"@lang": "ar",
                                            "#value": "نص العنوان"
                                            }]
        field_data_struct_all_wrong_key = [
            {"@other": "en", "#value": "title text"},
            {"@lang": "ar", "text": "نص العنوان"}]
        field_data_string = ["title text 1", "نص العنوان"]

        self.assertEqual(
            data_transform.extract_components(
                field_data_struct_normal, {
                    "idenfifier": "11111"}, "any_file_name", "dc:title"), [
                [
                    "title text", "en"], ["نص العنوان", "ar"]],
            msg="Error in [field_data_struct_normal] case")
        self.assertEqual(
            data_transform.extract_components(
                field_data_struct_all_wrong_key,
                {
                    "identifier": "11111"},
                "any_file_name",
                "dc:title"),
            [],
            msg="Error in [field_data_struct_all_wrong_key] case")
        self.assertEqual(
            data_transform.extract_components(
                field_data_struct_one_wrong_key, {
                    "identifier": "11111"}, "any_file_name", "dc:title"), [
                ["نص العنوان", "ar"]],
            msg="Error in [field_data_struct_one_wrong_key] case")
        self.assertEqual(
            data_transform.extract_components(
                field_data_string, {
                    "identifier": "11111"}, "any_file_name", "dc:title"), [
                [
                    "title text 1", ""], ["نص العنوان", ""]],
            msg="Error in [field_data_string] case")
        self.assertEqual(
            data_transform.extract_components([],
                                              {"identifier": "11111"},
                                              "any_file_name",
                                              "dc:title"),
            [], msg="Error in [field_data_all_null] case")

    def test_add_nested_field_to_es(self):
        es_doc = {"a_english": "text1"}
        self.assertEqual({"a_english": "text1",
                          "b_german": "der Text"},
                         data_transform.add_nested_field_to_es("b_german",
                                                               "der Text",
                                                               es_doc))
        self.assertEqual({"a_english": "text1|text2",
                          "b_german": "der Text"},
                         data_transform.add_nested_field_to_es("a_english",
                                                               "text2",
                                                               es_doc))

    def test_process_sub_field(self):
        self.assertEqual(data_transform.process_sub_field(["text", "en"],
                                                          "ge"),
                         ("text", "english"),
                         msg="Error in [inside-instance language found] case")
        self.assertEqual(data_transform.process_sub_field(
            ["text", ""],
            "ge"), ("text", "german"),
                         msg="Error in [inside-instance \
                                   language not found ] case")
        self.assertEqual(data_transform.process_sub_field(
            ["text", ""], ""), ("text", conf.default_language),
                         msg="Error in [No language] case")

    def test_process_field(self):
        field_data_no_language = ["title text 1", "hello world"]
        field_data_with_language = [{"@lang": "en", "#value": "title text 1"},
                                    {"@lang": "de",
                                     "#value": "ich bin in Göttingen"}]
        field_data_duplicate_language = [{"@lang": "en",
                                          "#value": "title text 1"},
                                         {"@lang": "de",
                                          "#value": "ich bin in Göttingen"
                                          },
                                         {"@lang": "en",
                                          "#value": "title text 2"
                                          }
                                         ]
        self.assertEqual(data_transform.process_field("dc:title",
                                                      field_data_no_language,
                                                      "any_file_name",
                                                      {"identifier": "11111"},
                                                      "en"),
                         {"dc:title_english": "title text 1|hello world"},
                         msg="Errot in [field_data_no_language] case")
        self.assertEqual(data_transform.process_field("dc:title",
                                                      field_data_with_language,
                                                      "any_file_name",
                                                      {"identifier": "11111"},
                                                      "en"),
                         {"dc:title_english": "title text 1",
                          "dc:title_german": "ich bin in Göttingen"},
                         msg="Error in [field_data_with_language] case")
        self.assertEqual(data_transform.process_field(
            "dc:title",
            field_data_duplicate_language,
            "any_file_name",
            {"identifier": "11111"},
            "en"),
                         {"dc:title_english": "title text 1|title text 2",
                          "dc:title_german": "ich bin in Göttingen"},
                         msg="Error in [field_data_duplicate_language] case")

    def test_process_line(self):
        line_data_in_field_lang = {
            "ore:Proxy": [{'dc:language': ['de'],
                           'edm:europeanaProxy': ['true'],
                           "dc:title":["not candidate title"]},
                          {'dc:language': ['de'],
                           'edm:europeanaProxy': ['false'],
                           "dc:title":[{"@lang": "en",
                                        "#value": "candidate title"
                                        }
                                       ]
                           }
                          ],
            'edm:EuropeanaAggregation': [{'edm:language': ['de']}],
            "identifier": "9999"
            }

        line_data_out_proxy_lang = {
            "ore:Proxy": [{'dc:language': [''],
                           'edm:europeanaProxy': ['true'],
                           "dc:title":["not candidate title"]},
                          {'dc:language': ['de'],
                           'edm:europeanaProxy': ['false'],
                           "dc:title":["ich bin aus deutschland"]}],
            'edm:EuropeanaAggregation': [{'edm:language': ['de']}],
            "identifier": "9999"}
        line_data_with_one_missing_lang = {
            "ore:Proxy": [{'edm:europeanaProxy': ['true'],
                           "dc:title":["not candidate title"]},
                          {'dc:language': ['de'],
                           'edm:europeanaProxy': ['false'],
                           "dc:title":[{"@lang": "en",
                                        "#value": "candidate title"
                                        }
                                       ],
                           "dc:description": ["das ist deutsch"]
                           }
                          ],
            'edm:EuropeanaAggregation': [{'edm:language': ['mul']}],
            "identifier": "9999"}

        self.assertEqual(
            data_transform.process_line(
                line_data_in_field_lang, "any_file_name"), {
                "doc_id": line_data_in_field_lang['identifier'],
                "doc_source": "any_file_name",
                "dc:title": {
                    "dc:title_english": "candidate title"
                    }
                }, msg="Error in [line_data_in_field_lang] case")

        self.assertEqual(
            data_transform.process_line(
                line_data_out_proxy_lang,
                "any_file_name"),
            {
                "doc_id": line_data_out_proxy_lang['identifier'],
                "doc_source": "any_file_name",
                "dc:title": {
                    "dc:title_german": "ich bin aus deutschland"}},
            msg="Error in [line_data_out_proxy_lang] case")

        self.assertEqual(
            data_transform.process_line(
                line_data_with_one_missing_lang, "any_file_name"), {
                    "doc_id": line_data_with_one_missing_lang['identifier'],
                    "doc_source": "any_file_name",
                    "dc:title": {
                        "dc:title_english": "candidate title"
                        },
                    "dc:description": {
                        "dc:description_unknown": "das ist deutsch"
                        }
                    }, msg="Error in [line_data_with_two_fields] case")

    def test_transform_data(self):
        line_data = {"ore:Proxy": [{'edm:europeanaProxy': ['true'],
                                    "dc:title":["not candidate title"]},
                                   {'dc:language': ['de'],
                                    'edm:europeanaProxy': ['false'],
                                    "dc:title":[{"@lang": "en",
                                                 "#value": "candidate title"}],
                                    "dc:description": ["das ist deutsch"]}],
                     'edm:EuropeanaAggregation': [{'edm:language': ['de']}],
                     "identifier": "9999"}
        this_node = socket.gethostname()
        self.assertEqual(
            data_transform.transform_data(line_data), {
                "doc_id": line_data['identifier'],
                "doc_source": this_node,
                "dc:title": {
                    "dc:title_english": "candidate title"
                    },
                "dc:description": {
                    "dc:description_german": "das ist deutsch"
                    }
                }, msg="Error in [test_transform_data] case")

    def test_parse_json(self):
        valid_json = '{"a": "aaaa" , "b": "99"}'
        invalid_json = '{"a" : "aaaaa", "b" , "c": "100"}'
        self.assertEqual({"a": "aaaa", "b": "99"}, data_transform.parse_json(
            valid_json), msg="Error in [test_parse_json valid_json] case")
        self.assertEqual(
            None,
            data_transform.parse_json(invalid_json),
            msg="Error in [test_parse_json invalid_json] case")
        self.assertRaises(ValueError)

    def test_main(self):
        es = elasticsearch.Elasticsearch(
            hosts=conf.es_hosts.split(","), timeout=500)
        es.indices.create(index="test-index-xyz-78910",
                          body=index_settings.index_setting
                          )
        es.indices.put_mapping(
            index="test-index-xyz-78910",
            doc_type="resources",
            body=index_settings.index_mapping)
        submit_command = "".join(["bash /usr/local/spark/bin/spark-submit\
                              --master spark://", conf.spark_master, " \
                              --name 'Testing transform and indexing job' \
                              --executor-memory 2g ", "data_transform.py  ",
                                  conf.test_data_path,
                                  " test-index-xyz-78910 resources"])
        args = shlex.split(submit_command)
        p = subprocess.call(args)

        self.assertEqual(
            True,
            es.indices.exists(
                index="test-index-xyz-78910"),
            msg="Error in [main_transform_and_indexing\
                   - test whether the index was created")
        es.indices.refresh(index="test-index-xyz-78910")
        self.assertEqual(
            3,
            es.count(
                index="test-index-xyz-78910",
                doc_type="resources")["count"],
            msg="Error in [test_data_transform_main - check doc count] result")

if __name__ == '__main__':
    unittest.main(warnings='ignore')
