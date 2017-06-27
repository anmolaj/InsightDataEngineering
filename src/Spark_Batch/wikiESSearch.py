from elasticsearch import Elasticsearch
import hashlib

es_cluster=["ec2-34-205-123-236.compute-1.amazonaws.com","ec2-34-226-76-219.compute-1.amazonaws.com","ec2-34-226-104-234.compute-1.amazonaws.com"]

es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))


if __name__ == "__main__":
	query={"query": {
        "match": {
            "content": "this"
        }
    }
}
	# res = es.search(index="tech",doc_type="classified",body=query)
	# print res
	# res = es.search(index="entertainment",doc_type="classified",body=query)
	# print res
	res = es.search(index="tech", body={"query": {"match_all": {}}})
	print res
	res = es.search(index="entertainment", body={"query": {"match_all": {}}})
	print res
	res = es.search(index="politics", body={"query": {"match_all": {}}})
	print res
	res = es.search(index="business", body={"query": {"match_all": {}}})
	print res