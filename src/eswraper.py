import json
import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class ESWraper:
    def __init__(self, idx_name="sogou"):
        self.idx_name = idx_name
        self.id_cnt = 0
        self.es = Elasticsearch([{'host': 'localhost', 'port':9200}])
        if self.es.ping():
            print("You connect")
            self.create_index()
        else:
            print("Couldn't connect")
        

    def create_index(self):
        body = {
            "mappings": {
                '_doc': {
                    "properties": {
                        "content": {
                            "type": "text",
                            "analyzer": "whitespace"
                        }
                    }
                }
            }
        }
        if not self.es.indices.exists(self.idx_name):
            self.es.indices.create(index=self.idx_name, body=body)
            print("Create index")
        else:
            print("Index already exists")

    def delete_index(self, idx_name):
        self.es.indices.delete(index=idx_name)
        print(idx_name + " deleted")

    def insert_by_single(self, piece):
        self.id_cnt += 1
        self.es.index(index=self.idx_name, doc_type="_doc", body={"content": "hello world"})
        

    def insert_by_bulk(self, filename):
        f = open(filename, encoding='utf-8')
        lines = f.readlines()
        action = []
        total = len(lines)
        for idx, line in enumerate(lines):
            # print(line)
            self.id_cnt += 1
            action.append({
                "_index": self.idx_name,
                "_type": "_doc",
                "_id": self.id_cnt,
                "_source": {
                    "content": line.strip()
                }
            })
            if(idx % 50000 == 0):
                print("\r %d out of total %d lines" % (idx, total), end="")
        bulk(self.es, action)

    def build_word_re(self, word, pos_list):
        if pos_list == None:
            return word + "/.|"
        elif len(pos_list) == 1:
            return word + "/" + pos_list[0] + "|"
        else:
            return "(" + word + "/(" + "|".join(pos_list) +"))" + "|"


    def search_docs(self, keywords, pos=None, size=300):
        # keys = " ".join(keywords)
        print(pos)
        regexp = str()
        for word in keywords:
            regexp += self.build_word_re(word, pos)
        regexp = regexp[:-1]
        print(regexp)
        print(self.idx_name)
        body = {
            "size": size,
            "query": {
                # "match_phrase": {
                #     "content": "东北/s"
                # }
                "regexp": {
                    "content": {
                        "value" : regexp
                    }
                }
            }
        }
        docs = self.es.search(index=self.idx_name, body=body)
        print(len(docs))
        return docs
    
if __name__ == "__main__":
    esWraper = ESWraper(idx_name="sogou")
    
    # esWraper.delete_index("sogou")
    # esWraper.insert_by_single("如")
    # esWraper.insert_by_single("如果\n")

    # for i in range(1, 12):
    #     print(i)
    #     esWraper.insert_by_bulk("/Volumes/Elements/result-wc/rmrb/rmrb2_"+str(i)+".txt")
    
    docs = esWraper.search_docs(["信息", "检索"], None)
    print(docs)

