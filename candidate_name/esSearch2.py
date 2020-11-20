import datetime

from elasticsearch import Elasticsearch


class esSearch():

    def __init__(self, es, index_name):
        self.es = es
        self.index_name = index_name

    def brand_search(self, keyword, i, max_index):
        body = {
            'query': {
                'bool': {
                    'must': [
                        {'match': {'brand': keyword}},
                    ],
                    'filter': [
                        {'term': {'max_index': max_index}},
                        {'term': {'word_num': i}},
                    ]
                }
            },
            "sort": [
                {"common": {"order": "desc"}},
            ],
            "from": 0,
            "size": 100,
            "_source": {"include": ["*"]}
        }
        json_res = self.es.search(index=self.index_name, body=body)
        return json_res

    def brand_search_fivenum(self, keyword, max_index):
        # 五个字以上的商标
        body = {
            'query': {
                'bool': {
                    'must': [
                        {'match': {'brand': keyword}},
                    ],
                    'filter': [
                        {'term': {'max_index': max_index}},
                        {'range': {'word_num': {'gte': 5}}}
                    ]
                }
            },
            "sort": [
                {"common": {"order": "desc"}},
            ],
            "from": 0,
            "size": 100,
            "_source": {"include": ["*"]}
        }
        json_res = self.es.search(index=self.index_name, body=body)
        return json_res

    def one_to_three(self, keyword, i):
        # 1到3星的商标
        body = {
            'query': {
                'bool': {
                    'must': [
                        {'match': {'brand': keyword}},
                    ],
                    'filter': [
                        {'term': {'word_num': i}},
                        {'range': {'max_index': {
                            'gte': 1,
                            'lte': 3
                        }}}
                    ]
                }
            },
            "sort": [
                {"common": {"order": "desc"}},
            ],
            "from": 0,
            "size": 100,
            "_source": {"include": ["*"]}
        }
        json_res = self.es.search(index=self.index_name, body=body)
        return json_res

    def one_to_three_fivenum(self, keyword):
        # 1到3星5个字以上的商标
        body = {
            'query': {
                'bool': {
                    'must': [
                        {'match': {'brand': keyword}},
                    ],
                    'filter': [
                        {'range': {'max_index': {
                            'gte': 1,
                            'lte': 3
                        }}},
                        {'range': {'word_num': {
                            'gte': 5,
                        }}}]

                },

            },
            "sort": [
                {"common": {"order": "desc"}},
            ],
            "from": 0,
            "size": 100,
            "_source": {"include": ["*"]}
        }
        json_res = self.es.search(index=self.index_name, body=body)
        return json_res

    def one_to_two(self, keyword, i):
        # 1到2星的商标
        body = {
            'query': {
                'bool': {
                    'must': [
                        {'match': {'brand': keyword}},
                    ],
                    'filter': [
                        {'term': {'word_num': i}},
                        {'range': {'max_index': {
                            'gte': 1,
                            'lte': 2
                        }}}
                    ]
                }
            },
            "sort": [
                {"common": {"order": "desc"}},
            ],
            "from": 0,
            "size": 100,
            "_source": {"include": ["*"]}
        }
        json_res = self.es.search(index=self.index_name, body=body)
        return json_res

    def one_to_two_fivenum(self, keyword):
        # 1到2星5个字以上的商标
        body = {
            'query': {
                'bool': {
                    'must': [
                        {'match': {'brand': keyword}},
                    ],
                    'filter': [
                        {'range': {'max_index': {
                            'gte': 1,
                            'lte': 3
                        }}},
                        {'range': {'word_num': {
                            'gte': 5,
                        }}}]
                }
            },
            "sort": [
                {"common": {"order": "desc"}},
            ],
            "from": 0,
            "size": 100,
            "_source": {"include": ["*"]}
        }
        json_res = self.es.search(index=self.index_name, body=body)
        return json_res

    def search_late_doc(self):
        # 查询最近新增加的数据
        body = {
            "query": {
                "range": {
                    "create_time": {
                        "gte": (datetime.datetime.now()-datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

                    }
                }
            },
            "from": 0,
            "size": 10000,
             "_source": ["used_brand"],
        }
        json_res = self.es.search(index=self.index_name, body=body)
        return json_res

if __name__ == '__main__':
    es = Elasticsearch(["http://10.168.3.132:9200/"])
    index_name = 'used_name'
    ES = esSearch(es, index_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print((datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"))
    list1 = []
    res = ES.search_late_doc()

    hits = res['hits']['hits']
    print(hits)
    for temp in hits:
        list1.append(temp['_source']['used_brand'])

    print(list1)
    # res = ES.brand_search('手', 2, 3)
    # res = ES.one_to_three('手', 2)
    # res = ES.one_to_two('手', 2)

    # hits1 = res['hits']['hits']
    # for temp1 in hits1:
    #     list1.append({'title': temp1['_source']['brand'], 'first': eval(temp1['_source']['group_index'])})
    #
    # print(list1)
