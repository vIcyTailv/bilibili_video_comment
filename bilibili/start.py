from multiprocessing import Pool
import os


def run():
    # 发送命令，启动一个爬虫
    cmd = "scrapy crawl reply_spider"
    os.system(cmd)


def main(number):
    # 创建进程池
    p = Pool(number)
    for n in range(number):
        p.apply_async(run)
    p.close()
    p.join()


if __name__ == '__main__':
    import sys
    # debug 设置开启 num 个进程
    # num = 16
    # 接收传入的参数，代表开启几个scrapy-redis进程
    num = sys.argv[1]
    num = int(num)
    print("开启[%s]个进程" % num)
    main(num)
    print("进程结束")
