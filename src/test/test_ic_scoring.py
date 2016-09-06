#!/usr/bin/python3
# -*- coding: utf-8 -*-
# europeana\metadataic\test\test_ic_scoring.py

import unittest
from unittest.mock import patch
import subprocess
import shlex

import elasticsearch
from elasticsearch import helpers
from pyspark import SparkContext

import ic_scoring
import conf
import index_settings


class ICScoringTestCase(unittest.TestCase):
    count = 0
    doc = {
        'identifier': '99999',
        'title': 'title text',
        'description': 'description'
    }

    @classmethod
    def setUpClass(self):
        """Called once, before any tests."""
        self.es = elasticsearch.Elasticsearch(
            hosts=conf.es_hosts.split(","), timeout=500)

    @classmethod
    def tearDownClass(self):
        """Called once, after all tests, if setUpClass successful."""
        self.es.indices.delete(index="any_index0000")
        self.es.indices.delete(index="any_index1111")
        self.es.indices.delete(index="any_index2222")
        self.es.indices.delete(index="test-index-xyz-78910")
        self.es.indices.delete(index="test-index-xyz-78910-results")
        print("test finished")

    @patch('ic_scoring.get_input')
    def test_prompt(self, mock_get_input):
        mock_get_input.side_effect = ["y", "N", "Any", "Y"]
        self.assertEqual(1, ic_scoring.prompt("any message"),
                         msg="Error in [test_prompt - 'y' input")
        self.assertEqual(0, ic_scoring.prompt("any message"),
                         msg="Error in [test_prompt - 'N' input")
        ic_scoring.prompt("any message")
        self.assertRaises(
            ValueError,
            msg="Error in [test_prompt - wrong input")

    @patch('ic_scoring.prompt')
    def test_init_index(self, mock_prompt):
        # The index does not exist
        res = ic_scoring.init_index(
            "any_index0000",
            "resources",
            index_settings.ic_index_setting,
            index_settings.ic_index_mapping)
        self.assertFalse(mock_prompt.called,
                         msg="Error in [test_init_index - no prompt] result")
        self.assertEqual(
            res,
            True,
            msg="Error in [test_init_index - not exists] result")
        self.assertTrue(
            self.es.indices.exists(
                index="any_index0000"),
            msg="Error in [test_init_index not exists- \
            check after creation] result")

        self.es.index(
            index="any_index0000",
            doc_type="resources",
            id=1,
            body=self.doc)
        self.es.indices.refresh(index="any_index0000")
        self.assertEqual(
            1,
            self.es.count(
                index="any_index0000",
                doc_type="resources")['count'],
            msg="Error in [test_init_index not exists -\
            check count after 1 doc] result")

        # The index exists, but create a new one 
        mock_prompt.side_effect = [True]
        res = ic_scoring.init_index(
            "any_index0000",
            "resources",
            index_settings.ic_index_setting,
            index_settings.ic_index_mapping)
        self.assertEqual(
            res,
            True,
            msg="Error in [test_init_index prompt_true] result")
        self.assertEqual(0, self.es.count(index="any_index0000",
                                          doc_type="resources")['count'],
                         msg="Error in [test_init_index  prompt true -\
                         check count] result")
        self.es.index(
            index="any_index0000",
            doc_type="resources",
            id=1,
            body=self.doc)
        self.es.indices.refresh(index="any_index0000")
        self.assertEqual(
            1,
            self.es.count(
                index="any_index0000",
                doc_type="resources")['count'],
            msg="Error in [test_init_index prompt true -\
            check count after 1 doc] result")

        # The index exists, no create, but append
        mock_prompt.side_effect = [False, True]
        res = ic_scoring.init_index(
            "any_index0000",
            "resources",
            index_settings.ic_index_setting,
            index_settings.ic_index_mapping)
        self.assertEqual(
            res,
            True,
            msg="Error in [test_init_index - prompt_false_true] result")
        self.es.index(
            index="any_index0000",
            doc_type="resources",
            id=2,
            body=self.doc)
        self.es.indices.refresh(index="any_index0000")
        self.assertEqual(
            2,
            self.es.count(
                index="any_index0000",
                doc_type="resources")['count'],
            msg="Error in [test_init_index prompt_false_true -\
            check count must be 2 doc] result")

        # The index exists, no create, no append
        mock_prompt.side_effect = [False, False]
        res = ic_scoring.init_index(
            "any_index0000",
            "resources",
            index_settings.ic_index_setting,
            index_settings.ic_index_mapping)
        self.assertEqual(
            res,
            False,
            msg="Error in [test_init_index exists -\
            prompt_false_false] result")

    def test_create_index(self):
        # without mapping
        self.assertFalse(
            self.es.indices.exists(
                index="any_index1111"),
            msg="Error in [test_create_index - without mapping -\
            assert before creation] result")
        res = ic_scoring.create_index(
            "any_index1111",
            "resources",
            index_settings.ic_index_setting,
            "")
        self.assertTrue(
            self.es.indices.exists(
                index="any_index1111"),
            msg="Error in [test_create_index - without mapping -\
            assert after creation] result")
        self.assertTrue(
            res, msg="Error in [test_create_index - without mapping] result. ")

        # with mapping
        self.assertFalse(
            self.es.indices.exists(
                index="any_index2222"),
            msg="Error in [test_create_index - with mapping -\
            assert before creation] result")
        ic_scoring.create_index(
            "any_index2222",
            "resources",
            index_settings.ic_index_setting,
            index_settings.ic_index_mapping)
        self.assertTrue(
            self.es.indices.exists(
                index="any_index1111"),
            msg="Error in [test_create_index - with mapping -\
            assert after creation] result")
        self.assertTrue(
            res, msg="Error in [test_create_index - with mapping] result.")

    @patch('ic_scoring.es')
    def test_create_index_with_exception(self, mock_es):
        mock_es.indices.create.side_effect = Exception('Unexpected error')
        res = ic_scoring.create_index(
            "any_index3333",
            "resources",
            index_settings.ic_index_setting,
            index_settings.ic_index_mapping)
        self.assertRaises(
            Exception,
            ic_scoring.create_index,
            msg="Error in [test_create_index_with_exception -\
            assert excep. raises")
        self.assertFalse(
            res, msg="Error in [test_create_index_with_exception] result.")

    def test_docs_count(self):
        self.es.index(
            index="any_index1111",
            doc_type="resources",
            id=1,
            body=self.doc)
        self.es.index(
            index="any_index1111",
            doc_type="resources",
            id=2,
            body=self.doc)
        res = ic_scoring.docs_count("any_index1111", "resources")
        self.assertEqual(2, res, msg="Error in [test_docs_count] result")

    @patch('ic_scoring.es')
    def test_docs_count_with_exception(self, mock_es):
        mock_es.count.side_effect = Exception('Unexpected error')
        res = ic_scoring.docs_count("any_index1111", "resources")
        self.assertEqual(
            None,
            res,
            msg="Error in [test_docs_count_with_exception] result")

    def test_main(self):
        ic_index_name = "test-index-xyz-78910-results"
        self.es.indices.create(index=ic_index_name,
                               body=index_settings.ic_index_setting
                               )
        self.es.indices.put_mapping(index=ic_index_name,
                                    doc_type="resources",
                                    body=index_settings.ic_index_mapping)
        submit_command = "".join(["bash /usr/local/spark/bin/spark-submit\
                              --master spark://", conf.spark_master, "\
                              --name 'Test IC scoring job'\
                              --jars /usr/local/spark/jars/elasticsearch-hadoop-2.3.2.jar\
                              --executor-memory 2g ",
                                  "ic_scoring.py test-index-xyz-78910 \
                                  resources test-index-xyz-78910-results \
                                  resources 200"])
        args = shlex.split(submit_command)
        p = subprocess.call(args)
        self.assertTrue(self.es.indices.exists(index=ic_index_name),
                        msg="Error in [test_main-\
                        check after creation] result")
        self.es.indices.refresh(index=ic_index_name)
        self.assertEqual(
            3,
            self.es.count(
                index=ic_index_name,
                doc_type="resources")['count'],
            msg="Error in [test_ic_scoring_main - check count must be 3 doc] result")

        
    @patch('ic_scoring.get_index_name')
    def test_scoring(self, mock_get_index_name):
        doc = self.es.search(
            index="test-index-xyz-78910",
            body={
                "query": {
                    "match": {
                        "doc_id": "2021108/98"}}})["hits"]["hits"][0]
        doc_es_out_format = (doc["_id"], doc["_source"])
        mock_get_index_name.return_value = "test-index-xyz-78910"
        res = ic_scoring.scoring(doc_es_out_format)
        self.assertEqual(
            res[0],
            doc_es_out_format[0],
            msg="Error in [test_scoring - check _id] result")
        self.assertEqual(
            res[1]["doc_id"],
            "2021108/98",
            msg="Error in [test_scoring - check doc_id] result")
        self.assertEqual(
            float(
                format(
                    res[1]["dc:title"]["field_ic_score"],
                    ".2f")),
            27.46,
            msg="Error in [test_scoring - check field_ic_score] result")
        self.assertTrue(
            "dc:title_croatian" in res[1]["dc:title"]["sub_tfidf"],
            msg="Error in [test_scoring - check nested field] result")
        self.assertEqual(
            float(
                format(
                    res[1]["doc_ic_score"],
                    ".2f")),
            3.35,
            msg="Error in [test_scoring - check doc_ic_score] result")

    def test_score_instance(self):
        doc = self.es.search(
            index="test-index-xyz-78910",
            body={
                "query": {
                    "match": {
                        "doc_id": "2021108/98"}}})["hits"]["hits"][0]
        res = ic_scoring.score_instance(doc)
        self.assertEqual(
            res["_id"],
            doc["_id"],
            msg="Error in [test_score_instance - check _id] result")
        self.assertEqual(
            res["_source"]["doc_id"],
            doc["_source"]["doc_id"],
            msg="Error in [test_scoring - check doc_id] result")
        self.assertEqual(
            float(
                format(
                    res["_source"]["dc:title"]["field_ic_score"],
                    ".2f")),
            27.46,
            msg="Error in [test_scoring - check field_ic_score] result")
        self.assertTrue(
            "dc:title_croatian" in res["_source"]["dc:title"]["sub_tfidf"],
            msg="Error in [test_scoring - check nested field] result")
        self.assertEqual(
            float(
                format(
                    res["_source"]["doc_ic_score"],
                    ".2f")),
            3.35,
            msg="Error in [test_scoring - check doc_ic_score] result")

        doc_with_no_target_field = {"_id": "012345",
                                    "_type": "resources",
                                    "_source": {
                                        "doc_source": "source1",
                                        "doc_id": "docid333",
                                        "ref": {
                                             "url": "www.google.com",
                                             "uri": "GTvfRAAokmUYWB6789SBB"
                                        }
                                    }
                                    }
        res = ic_scoring.score_instance(doc_with_no_target_field)
        self.assertEqual(
            float(
                format(
                    res["_source"]["doc_ic_score"],
                    ".2f")),
            0,
            msg="Error in [test_scoring - check doc_ic_score 0] result")

    @patch('ic_scoring.score_subfield')
    def test_score_field(self, mock_score_subfield):
        def score_subfield_side_effect(
                doc,
                field,
                child,
                field_tfidf,
                child_tfidf_list,
                info_density_list):
            if child == "dc:title_english":
                field_tfidf[child] = {}
                field_tfidf[child]["tfidf_score"] = 10
                field_tfidf[child]["info_density"] = 6
                field_tfidf[child]["terms_count"] = 4
                child_tfidf_list.append(10)
                info_density_list.append(6)
            elif child == "dc:title_german":
                field_tfidf[child] = {}
                field_tfidf[child]["tfidf_score"] = 20
                field_tfidf[child]["info_density"] = 12
                field_tfidf[child]["terms_count"] = 8
                child_tfidf_list.append(20)
                info_density_list.append(12)
            elif child == "desc_english":
                field_tfidf[child] = {}
                field_tfidf[child]["tfidf_score"] = 0
                field_tfidf[child]["info_density"] = 0
                field_tfidf[child]["terms_count"] = 0
                child_tfidf_list.append(0)
                info_density_list.append(0)
            return field_tfidf, child_tfidf_list, info_density_list

        unittest.TestCase.maxDiff = None
        prepared_doc = {"_id": "577577",
                        "_source": {"doc_id": "2017",
                                    "doc_source": "source1",
                                    "doc_ic_score": 0.0,
                                    "doc_info_density_score": 0.0
                                    }
                        }
        doc_with_two_subfields = {
            '_id': '577577',
            '_type': 'resources',
            '_source': {
                'dc:title': {
                    'dc:title_english': 'historical image and etc.',
                    'dc:title_german': 'historische Bild und usw.'},
                'doc_id': '2017',
                "doc_source": "source1"}}

        mock_score_subfield.side_effect = score_subfield_side_effect
        score_field_res = ic_scoring.score_field(
            doc_with_two_subfields, "dc:title", prepared_doc)
        expected_res = {"_id": '577577',
                        "_source": {"doc_id": "2017",
                                    "doc_source": "source1",
                                    "doc_ic_score": 30.0,
                                    "doc_info_density_score": 18.0,
                                    "dc:title": {
                                        "field_ic_score": 30,
                                        "field_info_density_score": 18,
                                        "sub_tfidf": {
                                            'dc:title_english':
                                            {'tfidf_score': 10,
                                             'terms_count': 4,
                                             'info_density': 6
                                             },
                                            'dc:title_german':
                                            {'tfidf_score': 20,
                                             'terms_count': 8,
                                             'info_density': 12
                                             }
                                        }
                                    }
                                    }
                        }
        self.assertEqual(
            score_field_res,
            expected_res,
            msg="Error in [score_subfield_side_effect -\
            normal two subfields] result")

        prepared_doc2 = {"_id": "577577",
                         "_source": {"doc_id": "2017",
                                     "doc_source": "source1",
                                     "doc_ic_score": 0.0,
                                     "doc_info_density_score": 0.0
                                     }
                         }
        doc_with_subfield_zero_term = {
            '_id': '577577',
            '_type': 'resources',
            '_source': {
                'desc': {
                    'desc_english': ''},
                'doc_id': '2017',
                "doc_source": "source1"}}
        score_field_res2 = ic_scoring.score_field(
            doc_with_subfield_zero_term, "desc", prepared_doc2)
        expected_res2 = {
            '_id': '577577',
            '_source': {
                'doc_info_density_score': 0.0,
                'desc': {
                    'field_ic_score': 0,
                    'field_info_density_score': 0,
                    'sub_tfidf': {
                        'desc_english': {
                            'tfidf_score': 0,
                            'terms_count': 0,
                            'info_density': 0}}},
                'doc_ic_score': 0.0,
                'doc_id': '2017',
                'doc_source': 'source1'}}
        self.assertEqual(
            score_field_res2,
            expected_res2,
            msg="Error in [score_subfield_side_effect -\
            empty subfield] result")


    @patch('ic_scoring.get_termvectors')
    def test_score_subfield(self, mock_get_termvectors):
        # test normal termvector
        mock_get_termvectors.return_value = {
            "term_vectors": {
                "title.title_english": {
                    "field_statistics": {"sum_doc_freq": 1344,
                                         "doc_count": 100,
                                         "sum_ttf": 1233
                                         },
                    "terms": {
                        "develop": {"doc_freq": 23,
                                    "ttf": 23,
                                    "term_freq": 1
                                    },
                        "door": {"doc_freq": 14,
                                 "ttf": 33,
                                 "term_freq": 3
                                 }
                        }
                    }
                }
            }

        doc = {"_id": "1234",
               "_index": "test-index-xyz-78910",
               "_type": "resources",
               "_source": {"doc_id": "4444",
                           "doc_source": "source4"
                           }
               }
        field_tfidf = {'dc:title_german':
                       {'tfidf_score': 20,
                        'terms_count': 8,
                        'info_density': 12
                        }
                       }
        tfidf_dict, tfidf_list, density_list = ic_scoring.score_subfield(
            doc, "title", "title_english", field_tfidf, [20], [12])
        self.assertTrue(
            "title_english" and "dc:title_german" in tfidf_dict,
            msg="Error in [test_score_subfield -\
            check result subfields] result")
        self.assertEqual(
            float(
                format(
                    tfidf_dict["title_english"]["tfidf_score"],
                    ".2f")),
            7.97,
            msg="Error in [test_score_subfield - check tfidf_score] result")
        formatted_tfidf_list = [float(format(elem, ".2f"))
                                for elem in tfidf_list]
        formatted_density_list = [float(format(elem, ".2f"))
                                  for elem in density_list]
        self.assertEqual(
            formatted_tfidf_list, [
                20.0, 7.97], msg="Error in [test_score_subfield -\
            check tfidf_list] result")
        self.assertEqual(
            formatted_density_list, [
                12.0, 3.98], msg="Error in [test_score_subfield -\
            check density_list] result")

        # test an empty termvector
        mock_get_termvectors.return_value = mock_get_termvectors.return_value = {
            "term_vectors": {}
            }
        tfidf_dict, tfidf_list, density_list = ic_scoring.score_subfield(
            doc, "title", "title_arabic", tfidf_dict, tfidf_list, density_list)
        self.assertTrue(
            "title_english" and
            "dc:title_german" and
            "title_arabic" in tfidf_dict,
            msg="Error in [test_score_subfield -\
            check result subfields with score 0 case] result")
        self.assertEqual(
            float(
                format(
                    tfidf_dict["title_arabic"]["tfidf_score"],
                    ".2f")),
            0.0,
            msg="Error in [test_score_subfield -\
            check tfidf_score with score 0 case] result")
        formatted_tfidf_list = [float(format(elem, ".2f"))
                                for elem in tfidf_list]
        formatted_density_list = [float(format(elem, ".2f"))
                                  for elem in density_list]
        self.assertEqual(formatted_tfidf_list, [20.0, 7.97, 0.0],
                         msg="Error in [test_score_subfield -\
                         check tfidf_list with score 0 case] result")
        self.assertEqual(formatted_density_list, [12.0, 3.98, 0.0],
                         msg="Error in [test_score_subfield -\
                         check density_list with score 0 case] result")


    def test_calculate_tfidf(self):

        # Test wit normal term vector
        t_vector = {"term_vectors":
                    {"title": {
                        "field_statistics": {
                            "sum_doc_freq": 70,
                            "doc_count": 100,
                            "sum_ttf": 88
                        },
                        "terms": {
                            "develop": {"doc_freq": 20,
                                        "ttf": 78,
                                        "term_freq": 4
                                        },
                            "door": {"doc_freq": 50,
                                     "ttf": 120,
                                     "term_freq": 2
                                     },
                            "car": {"doc_freq": 3,
                                    "ttf": 120,
                                    "term_freq": 8
                                    }
                        }
                    }
                    }
                    }
        tfidf, length = ic_scoring.calculate_tfidf(t_vector, "title")
        self.assertEqual(
            float(
                format(
                    tfidf,
                    ".2f")),
            37.65,
            msg="Error in [test_calculate_tfidf - check tfidf result] result")
        self.assertEqual(
            int(length),
            3,
            msg="Error in [test_calculate_tfidf - check length result] result")

    def test_get_termvectors(self):
        doc = self.es.search(
            index="test-index-xyz-78910",
            body={
                "query": {
                    "match": {
                        "doc_id": "2021108/98"}}})["hits"]["hits"][0]
        res = ic_scoring.get_termvectors(
            "test-index-xyz-78910",
            "resources",
            doc["_id"],
            "dc:title.dc:title_croatian",
            doc["_source"]["doc_id"],
            doc["_source"]["doc_source"])
        expected_t_vector = {
            "dc:title.dc:title_croatian": {
                "field_statistics": {
                    "sum_doc_freq": 68,
                    "doc_count": 3,
                    "sum_ttf": 81
                },
                "terms": {
                    "Bareza": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "Djelo": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "Glasovi": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "Hrvatskog": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "Lisinskog": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "Nikša": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "Opere": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "Vatroslava": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "Zagrebu": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "dirigent": {
                        "doc_freq": 2,
                        "ttf": 3,
                        "term_freq": 1
                    },
                    "i": {
                        "doc_freq": 2,
                        "ttf": 3,
                        "term_freq": 1
                    },
                    "izvode": {
                        "doc_freq": 2,
                        "ttf": 2,
                        "term_freq": 1
                    },
                    "kazališta": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "mješoviti": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "narodnog": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "opere": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "orkestar": {
                        "doc_freq": 2,
                        "ttf": 3,
                        "term_freq": 1
                    },
                    "solisti": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "u": {
                        "doc_freq": 2,
                        "ttf": 3,
                        "term_freq": 1
                    },
                    "ulomci": {
                        "doc_freq": 3,
                        "ttf": 4,
                        "term_freq": 1
                    },
                    "zagrebačke": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    },
                    "zbor": {
                        "doc_freq": 1,
                        "ttf": 1,
                        "term_freq": 1
                    }
                }
            }
        }
        self.assertEqual(
            res["term_vectors"],
            expected_t_vector,
            msg="Error in [test_get_termvectors -\
            check getting a termvector] result")

    @patch('ic_scoring.elasticsearch')
    def test_term_vectors_with_exception(self, mock_elasticsearch):
        mock_elasticsearch.Elasticsearch().termvectors.side_effect = Exception()
        a = ic_scoring.get_termvectors(
            "anyindexname",
            "anydoctype",
            "anyinternalid",
            "anyfield",
            "anydocid",
            "anydocsrc")
        mock_elasticsearch.Elasticsearch().termvectors.assert_called_with(
            index="anyindexname",
            doc_type="anydoctype",
            id="anyinternalid",
            dfs=True,
            offsets=False,
            payloads=False,
            positions=False,
            field_statistics=True,
            term_statistics=True,
            fields=["anyfield"])
        self.assertRaises(
            Exception,
            ic_scoring.get_termvectors,
            msg="Error in [test_term_vectors_with_exception] case")
        self.assertEqual(
            a, {}, msg="Error in [test_term_vectors_with_exception-\
            empty tvector] case")

    
    def test_es_replication(self):
        old_no_rep = int(self.es.indices.get_settings("test-index-xyz-78910")[
            "test-index-xyz-78910"]["settings"]["index"]["number_of_replicas"])
        self.assertEqual(
            old_no_rep, 0, msg="Error in [test_es_replication-\
            no of replicas befor] case")
        
        ic_scoring.es_replication("test-index-xyz-78910", 2)
        
        self.es.indices.refresh(index="test-index-xyz-78910")
        new_no_rep = int(self.es.indices.get_settings("test-index-xyz-78910")[
            "test-index-xyz-78910"]["settings"]["index"]["number_of_replicas"])
        self.assertEqual(
            new_no_rep, 2, msg="Error in [test_es_replication-\
            no of replicas after] case")
       
        
if __name__ == '__main__':
    unittest.main(warnings='ignore')
