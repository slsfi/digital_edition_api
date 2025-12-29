from flask import Blueprint, jsonify, request
import logging
import json
import requests
from elasticsearch import Elasticsearch

from sls_api.endpoints.generics import elastic_config, get_project_id_from_name

search = Blueprint('search', __name__)

logger = logging.getLogger("sls_api.search")

# Search functions, elasticsearch or otherwise


es = Elasticsearch([{'host': elastic_config['host'], 'port': elastic_config['port']}])

# ensure the elasticsearch logger is set to INFO
es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.INFO)


# Freetext search through ElasticSearch API
@search.route("<project>/search/freetext/<search_text>/<fuzziness>")
@reader_auth_required()
def get_freetext_search(project, search_text, fuzziness=1):
    logger.info("Getting results from elastic")
    if len(search_text) > 0:
        es_body = {
            "query": {
                "match": {
                    "textData": {
                        "query": search_text,
                        "fuzziness": fuzziness
                    }
                }
            },
            "highlight": {
                "fields": {
                    "textData": {}
                }
            }
        }
        res = es.search(index=str(project), body=es_body)
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


# Location search through ElasticSearch API
@search.route("<project>/search/location/<search_text>/")
@reader_auth_required()
def get_location_search(project, search_text):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_text) > 0:
        res = es.search(index='location', body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"name": {"query": str(search_text), "fuzziness": 1}}},
                        {"match": {"city": {"query": str(search_text), "fuzziness": 1}}},
                        {"match": {"country": {"query": str(search_text), "fuzziness": 1}}}
                    ],
                    "filter": {
                        "term": {
                            "project_id": project_id
                        }
                    },
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "name": {},
                    "city": {},
                    "country": {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


# Subject search through ElasticSearch API
@search.route("<project>/search/subject/<search_text>/")
@reader_auth_required()
def get_subject_search(project, search_text):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_text) > 0:
        res = es.search(index='subject', body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"first_name": {"query": str(search_text), "fuzziness": 1}}},
                        {"match": {"last_name": {"query": str(search_text), "fuzziness": 1}}},
                        {"match": {"full_name": {"query": str(search_text), "fuzziness": 1}}}
                    ],
                    "filter": {
                        "term": {
                            "project_id": project_id
                        }
                    },
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "first_name": {},
                    "last_name": {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


# Tag search through ElasticSearch API
@search.route("<project>/search/tag/<search_text>/")
@reader_auth_required()
def get_tag_search(project, search_text):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_text) > 0:
        res = es.search(index='tag', body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"name": {"query": str(search_text), "fuzziness": 1}}}
                    ],
                    "filter": {
                        "term": {
                            "project_id": project_id
                        }
                    },
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "name": {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


# User-defined search through ElasticSearch API
@search.route("<project>/search/user_defined/<index>/<field>/<search_text>/<fuzziness>/")
@reader_auth_required()
def get_user_defined_search(project, index, field, search_text, fuzziness):
    logger.info("Getting results from elastic")
    if len(search_text) > 0:
        res = es.search(index=str(index), body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {str(field): {"query": str(search_text), "fuzziness": int(fuzziness)}}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    str(field): {}
                }
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


@search.route("/<project>/search/suggestions/<search_string>/<limit>")
@reader_auth_required()
def get_search_suggestions(project, search_string, limit):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_string) > 0:
        res = es.search(index="tag,location,subject,song," + str(project), body={
            "size": limit,
            "indices_boost": [
                {"song": 2.0},
                {"subject": 2.0},
                {"location": 2.0},
                {"tag": 2.0}
            ],
            "_source": {
                "includes": [""]
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "multi_match": {
                                            "query": str(search_string),
                                            "type": "phrase_prefix",
                                            "fields": ["*"],
                                            "lenient": True
                                        }
                                    },
                                    {
                                        "match": {
                                            "project_id": str(project_id)
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "multi_match": {
                                            "query": str(search_string),
                                            "type": "phrase_prefix",
                                            "fields": ["*"],
                                            "lenient": True
                                        }
                                    },
                                    {
                                        "match": {
                                            "_index": str(project)
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            "highlight": {
                "fields": {
                    "name": {},
                    "full_name": {},
                    "song_name": {},
                    "message": {},
                    "textData": {}
                },
                "boundary_scanner": "word",
                "number_of_fragments": 1
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


@search.route("/<project>/search/all/<search_string>/<limit>")
@reader_auth_required()
def get_search_all(project, search_string, limit):
    logger.info("Getting results from elastic")
    project_id = get_project_id_from_name(project)
    if len(search_string) > 0:
        res = es.search(index="tag,location,subject,song," + str(project), body={
            "size": limit,
            "indices_boost": [
                {"song": 2.0},
                {"subject": 2.0},
                {"location": 2.0},
                {"tag": 2.0}
            ],
            "query": {
                "bool": {
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "multi_match": {
                                            "query": str(search_string),
                                            "type": "phrase_prefix",
                                            "fields": ["*"],
                                            "lenient": True
                                        }
                                    },
                                    {
                                        "match": {
                                            "project_id": str(project_id)
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "multi_match": {
                                            "query": str(search_string),
                                            "type": "phrase_prefix",
                                            "fields": ["*"],
                                            "lenient": True
                                        }
                                    },
                                    {
                                        "match": {
                                            "_index": str(project)
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            "highlight": {
                "fields": {
                    "name": {},
                    "full_name": {},
                    "song_name": {},
                    "message": {},
                    "textData": {}
                },
                "boundary_scanner": "sentence",
                "number_of_fragments": 1,
                "boundary_max_scan": 10
            }
        })
        if len(res['hits']) > 0:
            return jsonify(res['hits']['hits'])
        else:
            return jsonify("")
    else:
        return jsonify("")


@search.route("/<project>/search/elastic/<indexes>", methods=["POST"])
@reader_auth_required()
def get_search_elastic(project, indexes):
    request_data = request.get_json()
    query = json.dumps(request_data)
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    url = str('http://' + str(elastic_config['host']) + ':' + str(elastic_config['port']) + '/' + str(indexes) + '/_search?')
    response = requests.get(url, data=query, headers=headers)
    results = json.loads(response.text)
    return results


@search.route("/<project>/search/mtermvector/<indexes>/<terms>", methods=["POST"])
@reader_auth_required()
def get_terms_elastic(project, indexes, terms):
    request_data = request.get_json()
    query = json.dumps(request_data)
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    url = str('http://' + str(elastic_config['host']) + ':' + str(elastic_config['port']) + '/' + str(indexes) + '/_mtermvectors?')
    response = requests.get(url, data=query, headers=headers)
    results = json.loads(response.text)
    terms = str(terms).split(',')
    if results is not None:
        data = {}
        for term in terms:
            data[str(term)] = []
            for doc in results['docs']:
                if str(term) in doc['term_vectors']['textDataIndexed']['terms']:
                    data[str(term)].append({doc['_id']: doc['term_vectors']['textDataIndexed']['terms'][str(term)]})
        return jsonify(data)
    return results
