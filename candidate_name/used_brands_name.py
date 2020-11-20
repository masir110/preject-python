import datetime

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from elasticsearch import Elasticsearch
from sanic import Blueprint
from sanic.exceptions import ServerError
from sanic.response import json

from esSearch2 import esSearch

used_name = Blueprint('used_name')


@used_name.route('used_names', methods=['POST'])
async def used_brands_name(request):
    # 接受参数
    date = request.json
    date['create_time']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # print(date)
    # print(type(date))

    # 将数据保存进ES
    es = Elasticsearch(["http://10.168.3.132:9200/"])
    index_name = 'used_name'
    try:
        es.index(index=index_name, doc_type='used_name', body=date)
    except Exception:
        raise ServerError('ES Database index exception', status_code=500)

    return json({'message': 'ok'})


# 定时任务
executor = ThreadPoolExecutor(max_workers=10)
executors = {
    'default': executor
}
# 创建调度器
scheduler = BackgroundScheduler(executors=executors)


# 定义任务
def delete_from_candidate():
    """
    从candidate表中删除已经使用过的商标名称
    :return:
    """
    #查出最近一小时内新增的数据
    es = Elasticsearch(["http://10.168.3.132:9200/"])
    index_name = 'used_name'
    ES = esSearch(es, index_name)


# 添加任务
scheduler.add_job(delete_from_candidate, 'interval', hours=1)

# 启动
scheduler.start()
