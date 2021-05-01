from pymongo import MongoClient

MONGO_HOST = "192.168.228.100"
MONGO_PORT = 27017

# 建立数据库连接
client = MongoClient(MONGO_HOST, MONGO_PORT)
# 连接目标数据库
db = client["bilibili_data"]
# 连接集合
col_name = "b_video_stat_" + "20210425"
col = db[col_name]
fb = open("./aid.txt", "a")


def run(aid):
    data = col.find({"aid": aid})
    try:
         if data[0]:
            fb.write(str(aid))
            fb.write("\n")
    except:
        pass


def main(start, stop):
    for n in range(start, stop):
        print(n)
        run(n)
    fb.close()


if __name__ == '__main__':
    main(13610000, 13620000)
