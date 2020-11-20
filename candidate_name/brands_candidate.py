import random

from elasticsearch import Elasticsearch
from sanic import Blueprint
from sanic.response import json

from esSearch2 import esSearch
from sanic.exceptions import ServerError

cd = Blueprint('candidate')


@cd.route('/brands')
async def candidate(request):
    # 创建es对象
    # es = Elasticsearch(["http://10.168.3.132:9200/"])
    es = Elasticsearch(['es-cn-v6419gi82000giqwn.public.elasticsearch.aliyuncs.com'],
                       http_auth=('elastic', 'Zs101577'),
                       port=9200,
                       timeout=100
                       )
    index_name = 'candidate_name'
    index_name2 = 'candidate_poetry_name'
    ES = esSearch(es, index_name)
    ES_poetry = esSearch(es, index_name2)

    # return json({'test':'hello'})

    # 接收参数
    num = request.raw_args.get('num', None)
    keyword = request.raw_args.get('keyword', None)
    isauthen = request.raw_args.get('isauthen', None)

    # 校验参数
    if not all([num, isauthen, keyword]):
        return json({'error': 'Lack of required parameters'})

    if not isinstance(num, str):
        return json({'error': 'num type error'})

    if isauthen not in ['0', '1']:
        return json({'error': 'isauthen type error'})

    if not isinstance(keyword, str):
        return json({'error': 'keyword type error'})

    # 满足条件非诗词4星
    four_list = []
    # 满足条件诗词4星
    four_poetry_list = []
    # 满足条件诗词3星
    three_poetry_list = []
    # 满足条件非诗词1-3星
    one_to_three = []
    # 满足条件诗词1-3星
    one_to_three_poetry = []
    # 满足条件诗词1-2星
    one_to_two_poetry = []

    try:
        num = num.split(',')
        for i in num:
            if i != '5':
                res1 = ES.brand_search(keyword, i, 4)
                hits1 = res1['hits']['hits']
                for temp1 in hits1:
                    four_list.append(
                        {'title': temp1['_source']['brand'], 'first': eval(temp1['_source']['group_index'])})

                res2 = ES_poetry.brand_search(keyword, i, 4)
                hits2 = res2['hits']['hits']
                for temp2 in hits2:
                    four_poetry_list.append(
                        {'title': temp2['_source']['brand'], 'first': eval(temp2['_source']['group_index'])})

                res3 = ES_poetry.brand_search(keyword, i, 3)
                hits3 = res3['hits']['hits']
                for temp3 in hits3:
                    three_poetry_list.append(
                        {'title': temp3['_source']['brand'], 'first': eval(temp3['_source']['group_index'])})

                res4 = ES.one_to_three(keyword, i)
                hits4 = res4['hits']['hits']
                for temp4 in hits4:
                    one_to_three.append(
                        {'title': temp4['_source']['brand'], 'first': eval(temp4['_source']['group_index'])})

                res5 = ES_poetry.one_to_three(keyword, i)
                hits5 = res5['hits']['hits']
                for temp5 in hits5:
                    one_to_three_poetry.append(
                        {'title': temp5['_source']['brand'], 'first': eval(temp5['_source']['group_index'])})

                res6 = ES_poetry.one_to_two(keyword, i)
                hits6 = res6['hits']['hits']
                for temp6 in hits6:
                    one_to_two_poetry.append(
                        {'title': temp6['_source']['brand'], 'first': eval(temp6['_source']['group_index'])})

            else:
                res1 = ES.brand_search_fivenum(keyword, 4)
                hits1 = res1['hits']['hits']
                for temp1 in hits1:
                    four_list.append(
                        {'title': temp1['_source']['brand'], 'first': eval(temp1['_source']['group_index'])})

                res2 = ES_poetry.brand_search_fivenum(keyword, 4)
                hits2 = res2['hits']['hits']
                for temp2 in hits2:
                    four_poetry_list.append(
                        {'title': temp2['_source']['brand'], 'first': eval(temp2['_source']['group_index'])})

                res3 = ES_poetry.brand_search_fivenum(keyword, 3)
                hits3 = res3['hits']['hits']
                for temp3 in hits3:
                    three_poetry_list.append(
                        {'title': temp3['_source']['brand'], 'first': eval(temp3['_source']['group_index'])})

                res4 = ES.one_to_three_fivenum(keyword)
                hits4 = res4['hits']['hits']
                for temp4 in hits4:
                    one_to_three.append(
                        {'title': temp4['_source']['brand'], 'first': eval(temp4['_source']['group_index'])})

                res5 = ES_poetry.one_to_three_fivenum(keyword)
                hits5 = res5['hits']['hits']
                for temp5 in hits5:
                    one_to_three_poetry.append(
                        {'title': temp5['_source']['brand'], 'first': eval(temp5['_source']['group_index'])})

                res6 = ES_poetry.one_to_two_fivenum(keyword)
                hits6 = res6['hits']['hits']
                for temp6 in hits6:
                    one_to_two_poetry.append(
                        {'title': temp6['_source']['brand'], 'first': eval(temp6['_source']['group_index'])})
    except Exception:
        raise ServerError('ES Database query exception', status_code=500)

    # 判断用户是否认证
    if isauthen == '1':
        # 非诗词
        if len(four_list) < 10:
            results1 = four_list + one_to_three
        else:
            random.shuffle(four_list)
            results1 = four_list[:9] + one_to_three
        # 诗词
        if len(four_poetry_list) < 3:
            results2 = four_poetry_list + one_to_three_poetry
        else:
            random.shuffle(four_poetry_list)
            results2 = four_poetry_list[:3] + one_to_three_poetry

        # 最终列表
        random.shuffle(results1)
        results = results2 + results1

        if len(results) > 10:
            results = results[:10]

        return json({'num': num,
                     'keyword': keyword,
                     'isauthen': isauthen,
                     'total_count': len(results),
                     'results': results
                     })

    else:
        # 非诗词
        if len(four_list) > 0:
            random.shuffle(four_list)
            results1 = four_list[:1] + one_to_three
        else:
            results1 = one_to_three

        # 诗词
        if len(three_poetry_list) > 0:
            random.shuffle(three_poetry_list)
            results2 = three_poetry_list[:1] + one_to_two_poetry
        else:
            results2 = one_to_two_poetry

        # 最终列表
        random.shuffle(results1)
        results = results2 + results1

        if len(results) > 10:
            results = results[:10]

        return json({'num': num,
                     'keyword': keyword,
                     'isauthen': isauthen,
                     'total_count': len(results),
                     'results': results})
