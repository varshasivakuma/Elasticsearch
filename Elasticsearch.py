import pandas as pd
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host':'localhost','port': 9200,'scheme':'http'}],verify_certs=False)
def create_collection(collection_name):
    if not es.indices.exists(index=collection_name):
        es.indices.create(index=collection_name)
        print(f"Collection '{collection_name}' created.")
    else:
        print(f"Collection '{collection_name}' already exists.")
def get_emp_count(collection_name):
    count = es.count(index=collection_name)
    print(f"Employee count in '{collection_name}':{count['count']}")
    return count['count']
def index_data(collection_name, df):
    for index, row in df.iterrows():
        es.index(index=collection_name, id=row['Employee ID'], document=row.to_dict())
    print(f"Data indexed in collection '{collection_name}'.")
def del_emp_by_id(collection_name, emp_id):
    es.delete(index=collection_name, id=emp_id)
    print(f"Employee with ID '{emp_id}' deleted from '{collection_name}'.")
def search_by_column(collection_name, column_name, value):
    query = {
        "query": {
            "match": {
                column_name: value
            }
        }
    }
    results = es.search(index=collection_name, query=query)
    print(f"Search results for '{column_name}' = '{value}' in '{collection_name}':")
    for hit in results['hits']['hits']:
        print(hit['_source'])
def get_dep_facet(collection_name):
    query = {
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"  # Use .keyword for aggregations
                }
            }
        }
    }
    results = es.search(index=collection_name, body=query)
    print(f"Department facets for '{collection_name}':")
    for bucket in results['aggregations']['departments']['buckets']:
        print(f"{bucket['key']}: {bucket['doc_count']} employees")
if _name_ == "_main_":
    v_name_collection = 'Hash_varsha'
    v_phone_collection = 'Hash_1234'
    data_path = 'C:\Users\rosha\Downloads\Documents\Employee Sample Data 1.csv'
    df = pd.read_csv(data_path)
    create_collection(v_name_collection)
    create_collection(v_phone_collection)
    index_data(v_name_collection, df)
    index_data(v_phone_collection, df)
    get_emp_count(v_name_collection)
    search_by_column(v_name_collection, 'Department', 'IT')
    search_by_column(v_name_collection, 'Gender', 'Male')
    del_emp_by_id(v_name_collection, 'E02003')
    get_emp_count(v_name_collection)
    get_dep_facet(v_name_collection)
    get_dep_facet(v_phone_collection)
