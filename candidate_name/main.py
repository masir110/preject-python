import logging
import time

from elasticsearch import Elasticsearch
from sanic import Sanic
from sanic.response import json

from brands_candidate import cd
from esSearch2 import esSearch
from used_brands_name import used_name

logging.basicConfig(filename='./logs/server_{}.log'.format(time.strftime('%Y-%m-%d', time.localtime())),
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

app = Sanic(__name__)

app.blueprint(cd)
app.blueprint(used_name)

@app.route('/')
async def index(request):
    print('hello')
    return json({'test': 'hello'})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
