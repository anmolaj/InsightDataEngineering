from elasticsearch import Elasticsearch

def create_es_index(categories):
	cat_body= {
		'settings' : {
			'number_of_shards' : 3,
			'number_of_replicas' : 2
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
	es_cluster=["ec2-54-172-59-160.compute-1.amazonaws.com","ec2-54-89-133-252.compute-1.amazonaws.com","ec2-54-210-57-42.compute-1.amazonaws.com"]
	categories=['business','entertainment','politics','sport','tech']
	es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))
	create_es_index(categories)

