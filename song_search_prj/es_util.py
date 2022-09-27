import os
import time
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pymongo


class ES:
    def __init__(self):
        self._index = "music_data"
        self.es = Elasticsearch("http://127.0.0.1:9200")

    '''创建ES索引，确定分词类型'''

    def create_mapping(self):
        node_mappings = {
            "mappings": {
                "properties": {
                    "geci": {  # field: 歌词内容
                        "type": "text",  # lxw NOTE: cannot be string
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "index": "true"  # The index option controls whether field values are indexed.
                    },
                    "song": {  # field: 歌曲名称
                        "type": "text",  # lxw NOTE: cannot be string
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "index": "true"  # The index option controls whether field values are indexed.
                    },
                    "album": {  # field: 歌词所属专辑
                        "type": "text",  # lxw NOTE: cannot be string
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "index": "true"  # The index option controls whether field values are indexed.
                    },
                    "singer": {  # field: 歌手
                        "type": "text",  # lxw NOTE: cannot be string
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "index": "true"  # The index option controls whether field values are indexed.
                    },
                    "composer": {  # field: 歌手
                        "type": "text",  # lxw NOTE: cannot be string
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "index": "true"  # The index option controls whether field values are indexed.
                    },
                    "author": {  # field: 歌手
                        "type": "text",  # lxw NOTE: cannot be string
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "index": "true"  # The index option controls whether field values are indexed.
                    },
                }
            }
        }

        if not self.es.indices.exists(index=self._index):
            self.es.indices.create(index=self._index, body=node_mappings)
            print("Create {} mapping successfully.".format(self._index))
        else:
            print("index({}) already exists.".format(self._index))

    '''读取歌曲文件'''

    def read_data(self, music_file):
        index = 0
        count = 0
        action_list = []
        BULK_COUNT = 2000
        # start_time = time.time()
        with open(music_file, "r", encoding="utf-8") as f:
            tmp = f.readlines()
            print(len(tmp))
            for line in tmp:
                if not line:
                    continue
                item = json.loads(line)
                index += 1

                action = {
                    "_index": self._index,
                    "_source": {
                        "song": item["song"],
                        "singer": item['singer'],
                        "album": item['album'],
                        "geci": '\n'.join(item['geci']),
                        "compser": item['composer'],
                        "author": item['author']
                    }
                }

                action_list.append(action)

                if index > BULK_COUNT:
                    self.insert_data_bulk(action_list=action_list)
                    index = 0
                    count += 1
                    # print(count)
                    action_list = []

                # end_time = time.time()
                # print("Time Cost:{0}".format(end_time - start_time))

    '''批量插入数据'''

    def insert_data_bulk(self, action_list):
        success, _ = bulk(self.es, action_list, index=self._index, raise_on_error=True)
        print("Performed {0} actions. _: {1}".format(success, _))

    '''查询服务'''

    def search_specific(self, key, value, size):
        query_body = {
            "query": {
                "match": {
                    key: value
                }
            }
        }
        searched = self.es.search(index=self._index, body=query_body, size=size)

        # 输出查询的结果
        return searched["hits"]["hits"]
