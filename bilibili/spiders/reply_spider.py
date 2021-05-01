# -*- coding: utf-8 -*-
import scrapy
from scrapy_redis.spiders import RedisSpider
import logging
from scrapy.exceptions import CloseSpider
from bilibili.items import ReplyData
import json

logger = logging.getLogger(__name__)
class BilibiliReplySpider(RedisSpider):
    name = 'reply_spider'
    allowed_domains = ['bilibili.com']
    # 启动爬虫的命令
    redis_key = "reply_spider:start_urls"

    def parse(self, response):
        try:
            # 若settings中HTTPERROR_ALLOW_ALL = True，则需检测状态吗
            if response.status not in [200, 301, 302, 303, 307]:
                raise CloseSpider("网址:%s 状态码异常:%s" % (response.url, response.status))
        except CloseSpider as error:
            logger.error(error)
        else:
            try:
                # 解析json中data
                reply_data = json.loads(response.text)
            except Exception as error:
                # 若解析错误，记录url
                reply_data = {"code": 403}
                logger.error((response.url, error))
                with open("./error_json.txt", "a") as fb:
                    fb.write(response.url)
                    fb.write("\n")
            
            # # 取得评论区最大页数
            # pn_max = reply_data['page']['count'] // 20 + 1
            # pn = 1

            # for comment in reply_data['page']['count']:
                print(reply_data['data']['replies']['rpid'])

            if reply_data['code'] == 0:
                # 获取评论列表、评论oid、最大页数、当前页数
                replies_list = reply_data['data']['replies']
                replies_oid = replies_list[0]['oid']
                pn_max = reply_data['data']['page']['count']//20+1
                pn = reply_data['data']['page']['num']

                # 从评论列表中提取单条评论内容
                for reply in replies_list:
                    item = ReplyData()
                    item['oid']=replies_oid
                    item['rpid']=reply['rpid']
                    item['uname']=reply['member']['uname']
                    item['mid']=reply['mid']
                    item['message']=reply['content']['message']
                    # 传入item，进入pipelines
                    yield item

                # 拼接&pn={}至后续url上
                if reply_data['data']['page']['num'] <= pn_max:
                    next_url = "https://api.bilibili.com/x/v2/reply?type=1&sort=2&oid={}&pn={}".format(replies_oid,pn + 1)
                    yield scrapy.Request(next_url, callback=self.parse)
                    logger.info("跳转至：{}" .format(next_url))




