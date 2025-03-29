# Load cleaned data into Elasticsearch

from elasticsearch import Elasticsearch, helpers

es = Elasticsearch("LOAD URL HERE")

def load_to_elasticsearch(repos, index="github-open-source"):
    actions = [{
        "_index": index,
        "_id": f"{repo['owner']}/{repo['name']}",
        "_source": repo
        } for repo in repos]
    
    helpers.bulk(es, actions)
    
    