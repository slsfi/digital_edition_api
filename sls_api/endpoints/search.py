from flask.typing import ResponseReturnValue
from flask import Blueprint, jsonify, request
import logging
import json
import requests
from elasticsearch import Elasticsearch

from sls_api.endpoints.generics import elastic_config, get_project_id_from_name, reader_auth_required

search = Blueprint('search', __name__, url_prefix="/digitaledition")

logger = logging.getLogger("sls_api.search")

# Search functions, elasticsearch or otherwise
es = Elasticsearch([{'host': elastic_config['host'], 'port': elastic_config['port'], 'scheme': elastic_config.get('scheme', 'http')}])

# ensure the elasticsearch logger is set to INFO
es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.INFO)


# Freetext search through ElasticSearch API
@search.route("<string:project>/search/freetext/<string:search_text>/<string:fuzziness>")
@reader_auth_required()
def get_freetext_search(project: str, search_text: str, fuzziness: str = '1') -> ResponseReturnValue:
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
@search.route("<string:project>/search/location/<string:search_text>/")
@reader_auth_required()
def get_location_search(project: str, search_text: str) -> ResponseReturnValue:
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
@search.route("<string:project>/search/subject/<string:search_text>/")
@reader_auth_required()
def get_subject_search(project: str, search_text: str) -> ResponseReturnValue:
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
@search.route("<string:project>/search/tag/<string:search_text>/")
@reader_auth_required()
def get_tag_search(project: str, search_text: str) -> ResponseReturnValue:
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
@search.route("<string:project>/search/user_defined/<string:index>/<string:field>/<string:search_text>/<string:fuzziness>/")
@reader_auth_required()
def get_user_defined_search(project: str, index: str, field: str, search_text: str, fuzziness: str) -> ResponseReturnValue:
    logger.info("Getting results from elastic")
    if len(search_text) > 0:
        res = es.search(index=str(index), body={
            "size": 1000,
            "query": {
                "bool": {
                    "should": [
                        {"match": {str(field): {"query": str(search_text), "fuzziness": fuzziness}}}
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


@search.route("/<string:project>/search/suggestions/<string:search_string>/<int:limit>")
@reader_auth_required()
def get_search_suggestions(project: str, search_string: str, limit: int) -> ResponseReturnValue:
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


@search.route("/<string:project>/search/all/<string:search_string>/<int:limit>")
@reader_auth_required()
def get_search_all(project: str, search_string: str, limit: int) -> ResponseReturnValue:
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


@search.route("/<string:project>/search/elastic/<string:indexes>", methods=["POST"])
@reader_auth_required()
def get_search_elastic(project: str, indexes: str) -> ResponseReturnValue:
    request_data = request.get_json()
    query = json.dumps(request_data)
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    url = str('http://' + str(elastic_config['host']) + ':' + str(elastic_config['port']) + '/' + str(indexes) + '/_search?')
    response = requests.get(url, data=query, headers=headers)
    results = json.loads(response.text)
    return results


@search.route("/<string:project>/search/mtermvector/<string:indexes>/<string:terms>", methods=["POST"])
@reader_auth_required()
def get_terms_elastic(project: str, indexes: str, terms: str) -> ResponseReturnValue:
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
