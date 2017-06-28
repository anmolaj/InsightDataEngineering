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
	es_cluster=["<List-of-nodes>"]	
	categories=['business','entertainment','politics','sport','tech']
	es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))
	create_es_index(categories)

