from elasticsearch import Elasticsearch
import hashlib

es_cluster=["ec2-54-172-59-160.compute-1.amazonaws.com","ec2-54-89-133-252.compute-1.amazonaws.com","ec2-54-210-57-42.compute-1.amazonaws.com"]

es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))


if __name__ == "__main__":
	query={"query": {
        "match": {
            "content": "Anmol"
        }
    }
}
	res = es.search(index="tech",doc_type="classified",body=query)
	print res
	res = es.search(index="entertainment",doc_type="classified",body=query)
	print res