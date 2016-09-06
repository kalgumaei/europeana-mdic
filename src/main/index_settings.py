#!/usr/bin/python3
# -*- coding: utf-8 -*-
# europeana\metadataic\index_settings.py

import conf

# Elasticsearch available language analyzers
es_supported_languages = [
    'arabic',
    'armenian',
    'basque',
    'brazilian',
    'bulgarian',
    'catalan',
    'cjk',
    'czech',
    'danish',
    'dutch',
    'english',
    'finnish',
    'french',
    'galician',
    'german',
    'greek',
    'hindi',
    'hungarian',
    'indonesian',
    'irish',
    'italian',
    'latvian',
    'lithuanian',
    'norwegian',
    'persian',
    'portuguese',
    'romanian',
    'russian',
    'sorani',
    'spanish',
    'swedish',
    'turkish',
    'thai']

# Elasticsearch available snowball analyzers
es_snowball_analyzers = [
    'Armenian',
    'Basque',
    'Catalan',
    'Danish',
    'Dutch',
    'English',
    'Finnish',
    'French',
    'German',
    'Hungarian',
    'Italian',
    'Kp',
    'Lithuanian',
    'Lovins',
    'Norwegian',
    'Porter',
    'Portuguese',
    'Romanian',
    'Russian',
    'Spanish',
    'Swedish',
    'Turkish']

# language mapping dictionary
language_dict = {
    'ar': 'arabic',
    'be': 'belarusian',
    'bg': 'bulgarian',
    'br': 'breton',
    'bul': 'bulgarian',
    'ca': 'catalan',
    'cas': 'spanish',
    'cs': 'czech',
    'cu': 'slavonic',
    'cy': 'welsh',
    'cz': 'czech',
    'da': 'danish',
    'dan': 'danish',
    'danish': 'danish',
    'de': 'german',
    'deu': 'german',
    'du': 'dutch',
    'dut': 'dutch',
    'e': 'english',
    'el': 'greek',
    'elen': 'english',
    'ell': 'greek',
    'en': 'english',
    'eng': 'english',
    'english': 'english',
    'eo': 'esperanto',
    'es': 'spanish',
    'est': 'estonian',
    'et': 'estonian',
    'eu': 'basque',
    'fa': 'persian',
    'fi': 'finnish',
    'fin': 'finnish',
    'fr': 'french',
    'fra': 'french',
    'fre': 'french',
    'ga': 'irish',
    'ge': 'german',
    'ger': 'german',
    'german': 'german',
    'gez': 'geez',
    'gl': 'galician',
    'gr': 'greek',
    'grc': 'greek',
    'gre': 'greek',
    'he': 'hebrew',
    'hr': 'croatian',
    'hu': 'hungarian',
    'hun': 'hungarian',
    'hy': 'armenian',
    'is': 'icelandic',
    'it': 'italian',
    'ita': 'italian',
    'iw': 'hebrew',
    'japani': 'japanese',
    'ji': 'yiddish',
    'jp': 'japanese',
    'ka': 'georgian',
    'ko': 'korean',
    'la': 'latin',
    'lat': 'latin',
    'lt': 'lithuanian',
    'lv': 'latvian',
    'mk': 'macedonian',
    'ms': 'malay',
    'mt': 'maltese',
    'na': 'nauru',
    'nb': 'norwegian',
    'nds': 'german',
    'nl': 'dutch',
    'nld': 'dutch',
    'nn': 'norwegian',
    'no': 'norwegian',
    'nor': 'norwegian',
    'oc': 'occitan',
    'pl': 'polish',
    'pol': 'polish',
    'por': 'portuguese',
    'pr': 'spanish',
    'pt': 'portuguese',
    'ro': 'romanian',
    'ru': 'russian',
    'rus': 'russian',
    'sa': 'sanskrit',
    'se': 'northernsami',
    'si': 'sinhala',
    'sk': 'slovak',
    'sk-sk': 'slovak',
    'sl': 'slovenian',
    'slo': 'slovak',
    'slv': 'slovenian',
    'spa': 'spanish',
    'sq': 'albanian',
    'sr': 'serbian',
    'su': 'sundanese',
    'sv': 'swedish',
    'sw': 'swahili',
    'te': 'telugu',
    'tir': 'tigrinya',
    'tr': 'turkish',
    'uk': 'ukrainian',
    'ukr': 'ukrainian',
    'vi': 'vietnamese',
    'yi': 'yiddish',
    'zh': 'chinese'
}
# 'zxx': menas no linguistic information (added 2006-01-11)
# 'und': menas undetermined
# 'elen': unknown but temporary map it to english
# 'Org': uknown
# 'prt' : unknown or unsupported
# 'bmu' : unknown

def add_snowball_analyzer(sb_analyzer, analysis_setting):
    """Add a snowball analyzer to analysis setting."""

    analyzer = sb_analyzer.lower()
    analysis_setting["".join(["my_", analyzer])] = {
        "type": "snowball",
        "language": sb_analyzer,
        "stopword": []}

    return analysis_setting


def add_normal_analyzer(analyzer, analysis_setting):
    """Add a language analyzer to analysis setting."""

    analysis_setting["".join(["my_", analyzer])] = {
        "type": analyzer,
        "stopwords": []
    }
    return analysis_setting


def add_unknown_lang_analyzer(analysis_setting):
    """Add Standard analyzer to analysis setting."""

    analysis_setting["my_standard"] = {
        "type": "custom",
        "tokenizer": "standard",
        "char_filter": "html_strip",
        "stopwords": []
    }
    return analysis_setting


def generate_analysis_setting():
    """Generate analysis setting from given analyzers' lists."""
    analysis_setting = {}
    for analyzer in es_supported_languages:
        if analyzer.title() in es_snowball_analyzers:
            analysis_setting = add_snowball_analyzer(
                analyzer.title(), analysis_setting)
        else:
            analysis_setting = add_normal_analyzer(analyzer, analysis_setting)
    add_unknown_lang_analyzer(analysis_setting)
    return analysis_setting


def generate_mapping():
    """Generate field mapping from the given langauages."""

    index_mapping = {
        conf.type_name: {
            "_all": {
                "enabled": False
            },

            "dynamic_templates": [

                {"id": {
                    "match": "doc_id",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "string",
                        "store": True,
                        "index": "not_analyzed"
                    }
                }},
                {"src": {
                    "match": "doc_source",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "string",
                        "store": True,
                        "index": "not_analyzed"
                    }
                }}]}}
    
    for analyzer in es_supported_languages:
        index_mapping[conf.type_name]["dynamic_templates"].append(

            {analyzer[:3]: {
                "match": "*_" + analyzer,
                "match_mapping_type": "string",
                "mapping": {
                    "type": "string",
                    "store": True,
                    "analyzer": "my_" + analyzer
                }
            }})

    index_mapping[conf.type_name]["dynamic_templates"].append({"unknown": {
        "match": "*_*",
        "match_mapping_type": "string",
        "mapping": {
            "type": "string",
                    "store": True,
            "analyzer": "my_standard"
        }
    }})

    return (index_mapping)


# Original index setting and mapping
index_setting = {
    "settings": {
        "number_of_shards": 6,
        "number_of_replicas": 0,
        "refresh_interval": -1,
        "analysis": {
            "analyzer": generate_analysis_setting()
        }
    }
}
index_mapping = generate_mapping()



# Results index setting and mapping
ic_index_setting = {
    "settings": {
        "index": {
            "number_of_shards": 6,
            "number_of_replicas": 0,
            "refresh_interval": -1
        }
    }
}

ic_index_mapping = {
    conf.ic_type_name: {
        "_all": {
            "enabled": False
        },

        "dynamic_templates": [

            {"doc_id": {
                "match": "doc_id",
                "mapping": {
                         "type": "string",
                         "store": True,
                         "index": "not_analyzed"
                }
            }},
            {"doc_source": {
                "match": "doc_source",
                "mapping": {
                         "type": "string",
                         "store": True,
                         "index": "not_analyzed"
                }
            }},
            {"others": {
                "match_mapping_type": "double",
                "mapping": {
                    "type": "float",
                    "store": True,
                    "analyzer": "no"
                }
            }}
        ]

    }
}

