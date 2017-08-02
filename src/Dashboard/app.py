from flask import Flask,jsonify,request
from flask import render_template
import ast

from app import app

from elasticsearch import Elasticsearch

labels = []
values = []
word_labels = []
word_values = []
word_counts = []
label_words=[]

@app.route("/")
def get_chart_page():
	global labels,values
	labels = []
	values = []
	
	return render_template('chart.html', values=values, labels=labels)
@app.route('/refreshData')
def refresh_graph_data():
	global labels, values
	
	return jsonify(sLabel=labels, sData=values)

@app.route('/updateData', methods=['POST'])
def update_data():
	global labels, values
	if not request.form or 'data' not in request.form:
		return "error",400
	labels = ast.literal_eval(request.form['labels'])
	values = ast.literal_eval(request.form['data'])
	
	return "success",201

@app.route('/updateWordData', methods=['POST'])
def update_word_data():
	global word_labels, word_values, word_counts,label_words 
	if not request.form or 'word_data' not in request.form:
		return "error",400
	word_labels = ast.literal_eval(request.form['labels_word'])
	word_values = ast.literal_eval(request.form['word_data'])
	word_counts = ast.literal_eval(request.form['word_count'])
	label_words =zip(word_labels,word_values)
	
	
	return "success",201

@app.route('/entertainment')
def update_entertainment_data():
	global word_labels, word_values, word_counts,label_words 
	
	return render_template('entertainment.html', values=label_words)

@app.route('/business')
def update_business_data():
	global word_labels, word_values, word_counts,label_words 
	
	return render_template('business.html', values=label_words)

@app.route('/sport')
def update_sport_data():
	global word_labels, word_values, word_counts,label_words 
	
	return render_template('sport.html', values=label_words)

@app.route('/tech')
def update_tech_data():
	global word_labels, word_values, word_counts,label_words 

	return render_template('tech.html', values=label_words)

@app.route('/politics')
def update_politics_data():
	global word_labels, word_values, word_counts,label_words 

	return render_template('politics.html', values=label_words)

@app.route('/slides')
def get_slides():
	#global word_labels, word_values, word_counts,label_words 

	return render_template('https://docs.google.com/presentation/d/1_coyhE1g0bii5ksxXF5ZrVSsFeIRwepHLKS6eXqR1iE/pub?start=false&loop=false&delayms=3000', values=label_words)

@app.route('/search/<input_str>')
def search_ES(input_str):
	x,y=input_str.split("|")
	print (x,y)
	es_cluster=["ec2-34-205-123-236.compute-1.amazonaws.com:9200","ec2-34-226-76-219.compute-1.amazonaws.com:9200","ec2-34-226-104-234.compute-1.amazonaws.com:9200"]
	es = Elasticsearch(es_cluster, http_auth=('elastic','changeme'))
	query={'size':5,
                "query": {
        "term": {
            "content": y
        }
    }
}
	res = es.search(index=x, body=query)
	return render_template('search.html', values=res['hits']['hits'])

# if __name__ == "__main__":
# 	#app.run(host='http://ec2-34-225-80-111.compute-1.amazonaws.com', port=5001)
# 	app.run()
