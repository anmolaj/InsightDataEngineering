from elasticsearch import Elasticsearch
import hashlib

"""
This file is to check Elasticsearch database
"""

es_cluster=["<List-of-nodes>"]

es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))


if __name__ == "__main__":
	query={"query": {
        "match": {
            "content": "this"
        }
    }
}

	res = es.search(index="tech", body={"query": {"match_all": {}}})
	print res
	res = es.search(index="entertainment", body={"query": {"match_all": {}}})
	print res
	res = es.search(index="politics", body={"query": {"match_all": {}}})
	print res
	res = es.search(index="business", body={"query": {"match_all": {}}})
	print res