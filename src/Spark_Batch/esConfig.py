from elasticsearch import Elasticsearch

def create_es_index(categories):
	cat_body= {
		'settings' : {
			'number_of_shards' : 5,
			'number_of_replicas' : 3
		},
		'mappings': {
			"classified":{
				"properties":{
					"title":{"type" : "string"},
					"content":{"type" : "string"}

				}

			}
		
		}
	}

	for cat in categories:
		es.indices.create(index=cat,body=cat_body)


if __name__ == "__main__":
	es_cluster=["ec2-34-205-123-236.compute-1.amazonaws.com","ec2-34-226-76-219.compute-1.amazonaws.com","ec2-34-226-104-234.compute-1.amazonaws.com"]
	categories=['business','entertainment','politics','sport','tech']
	es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))
	create_es_index(categories)

