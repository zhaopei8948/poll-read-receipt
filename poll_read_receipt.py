import uuid, time, os, shutil, sched, pymysql, traceback, datetime
import xml.etree.cElementTree as ET
import operator as op
from concurrent.futures import ThreadPoolExecutor #线程池
from DBUtils.PooledDB import PooledDB


schedule = sched.scheduler(time.time, time.sleep)
delay = 3
receiveBackDir = r"/Users/zhaopei/Desktop/5/pdata/2"
receiveDir = r"/Users/zhaopei/Desktop/5/pdata/1"
xmlns = "{http://www.chinaport.gov.cn/ceb}"
pool = PooledDB(
creator=pymysql, # 使用链接数据库的模块
maxconnections=8, # 连接池允许的最大连接数，0和None表示不限制连接数
mincached=2, # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
maxcached=5, # 链接池中最多闲置的链接，0和None不限制
maxshared=0, # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
blocking=True, # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
maxusage=None, # 一个链接最多被重复使用的次数，None表示无限制
setsession=[], # 开始会话前执行的命令列表。如：[“set datestyle to …”, “set time zone …”]
ping=0,
# ping MySQL服务端，检查是否服务可用。
# 如：0 = None = never,
# 1 = default = whenever it is requested,
# 2 = when a cursor is created,
# 4 = when a query is executed,
# 7 = always
host='127.0.0.1',
port=3306,
user='root',
password='root',
database='bills',
charset='utf8'
)

thread_pool = ThreadPoolExecutor(4)


def fetchOne(sql, **kw):
    db = pool.connection()
    cursor = db.cursor()
    try:
        cursor.execute(sql, **kw)
    except Exception:
        traceback.print_exc()
        return None
    return cursor.fetchone()

def fetchAll(sql, **kw):
    db = pool.connection()
    cursor = db.cursor()
    try:
        cursor.execute(sql, **kw)
    except Exception:
        traceback.print_exc()
        return None
    return cursor.fetchall()

def sqlModify(sql, **kw):
    db = pool.connection()
    cursor = db.cursor()
    try:
        cursor.execute(sql, **kw)
        db.commit()
    except Exception:
        traceback.print_exc()
        db.rollback()

def getTextByTag(tree, tagName):
    tagObj = tree.find("%s%s" % (xmlns, tagName))
    if tagObj is None:
        return None
    else:
        return tagObj.text

def handleOrderReceipt(tree):
    print("开始处理订单回执")
    orderNo = getTextByTag(tree, "orderNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sorder_no, sreturn_status, sreturn_time, sreturn_info
    from t_order_head where sorder_no = '%s'
    ''' % (orderNo)
    print("开始执行：%s" % (sql))

    result = fetchOne(sql)

    print(result)
    if not result is None:
        sql = '''
        update t_order_head set sreturn_status = '%s',
        sstatus = '%s',
        sreturn_time = '%s',
        sreturn_info = '%s'
        where sorder_no = '%s'
        ''' % (returnStatus, returnStatus, returnTime, returnInfo, orderNo)
        if result[2] is None:
            print("开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                sqlModify(sql)

def handleLogisticsReceipt(tree):
    print("开始处理运单回执")
    logisticsNo = getTextByTag(tree, "logisticsNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select slogistics_no, sreturn_status, sreturn_time, sreturn_info
    from t_logistics where slogistics_no = '%s'
    ''' % (logisticsNo)
    print("开始执行：%s" % (sql))

    result = fetchOne(sql)

    print(result)
    if not result is None:
        sql = '''
        update t_logistics set sreturn_status = '%s',
        sstatus = '%s',
        sreturn_time = '%s',
        sreturn_info = '%s'
        where slogistics_no = '%s'
        ''' % (returnStatus, returnStatus, returnTime, returnInfo, logisticsNo)
        if result[2] is None:
            print("开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                sqlModify(sql)

def handlePaymentReceipt(tree):
    print("开始处理收款单回执")
    orderNo = getTextByTag(tree, "orderNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sorder_no, sreturn_status, sreturn_time, sreturn_info
    from t_payment where sorder_no = '%s'
    ''' % (orderNo)
    print("开始执行：%s" % (sql))

    result = fetchOne(sql)

    print(result)
    if not result is None:
        sql = '''
        update t_payment set sreturn_status = '%s',
        sstatus = '%s',
        sreturn_time = '%s',
        sreturn_info = '%s'
        where sorder_no = '%s'
        ''' % (returnStatus, returnStatus, returnTime, returnInfo, orderNo)
        if result[2] is None:
            print("开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                sqlModify(sql)

def handleInvtReceipt(tree):
    print("开始处理清单回执")
    orderNo = getTextByTag(tree, "orderNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sorder_no, sreturn_status, sreturn_time, sreturn_info
    from t_invt_head where sorder_no = '%s'
    ''' % (orderNo)
    print("开始执行：%s" % (sql))

    result = fetchOne(sql)

    print(result)
    if not result is None:
        sql = '''
        update t_invt_head set sreturn_status = '%s',
        sstatus = '%s',
        sreturn_time = '%s',
        sreturn_info = '%s'
        where sorder_no = '%s'
        ''' % (returnStatus, returnStatus, returnTime, returnInfo, orderNo)
        if result[2] is None:
            print("开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                sqlModify(sql)

def handleInvtCancelReceipt(tree):
    print("开始处理撤销清单回执")
    invtNo = getTextByTag(tree, "invtNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sinvt_no, sreturn_status, sreturn_time, sreturn_info
    from t_invt_cancel where sinvt_no = '%s'
    ''' % (invtNo)
    print("开始执行：%s" % (sql))

    result = fetchOne(sql)

    print(result)
    if not result is None:
        sql = '''
        update t_invt_cancel set sreturn_status = '%s',
        sstatus = '%s',
        sreturn_time = '%s',
        sreturn_info = '%s'
        where sinvt_no = '%s'
        ''' % (returnStatus, returnStatus, returnTime, returnInfo, invtNo)
        if result[2] is None:
            print("开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                sqlModify(sql)

def handleWayBillReceipt(tree):
    print("开始处理清单总分单回执")
    billNo = getTextByTag(tree, "billNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sbill_no, sreturn_status, sreturn_time, sreturn_info
    from t_waybill_head where sbill_no = '%s'
    ''' % (billNo)
    print("开始执行：%s" % (sql))

    result = fetchOne(sql)

    print(result)
    if not result is None:
        sql = '''
        update t_waybill_head set sreturn_status = '%s',
        sstatus = '%s',
        sreturn_time = '%s',
        sreturn_info = '%s'
        where sbill_no = '%s'
        ''' % (returnStatus, returnStatus, returnTime, returnInfo, billNo)
        if result[2] is None:
            print("开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                sqlModify(sql)

def handleDepartureReceipt(tree):
    print("开始离境单回执")
    copNo = getTextByTag(tree, "copNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sbill_no, sreturn_status, sreturn_time, sreturn_info
    from t_departure_head where scop_no = '%s'
    ''' % (copNo)
    print("开始执行：%s" % (sql))

    result = fetchOne(sql)

    print(result)
    if not result is None:
        sql = '''
        update t_departure_head set sreturn_status = '%s',
        sstatus = '%s',
        sreturn_time = '%s',
        sreturn_info = '%s'
        where scop_no = '%s'
        ''' % (returnStatus, returnStatus, returnTime, returnInfo, copNo)
        if result[2] is None:
            print("开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                sqlModify(sql)

def handleSummaryReceipt(tree):
    print("开始处理汇总单回执")
    copNo = getTextByTag(tree, "copNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sreturn_status, sreturn_time, sreturn_info
    from t_summary_head where scop_no = '%s'
    ''' % (copNo)
    print("开始执行：%s" % (sql))

    result = fetchOne(sql)

    print(result)
    if not result is None:
        sql = '''
        update t_summary_head set sreturn_status = '%s',
        sstatus = '%s',
        sreturn_time = '%s',
        sreturn_info = '%s'
        where scop_no = '%s'
        ''' % (returnStatus, returnStatus, returnTime, returnInfo, copNo)
        if result[1] is None:
            print("开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if op.lt(result[1], returnTime):
                print("开始执行：%s" % (sql))
                sqlModify(sql)

def handle900Receipt(tree):
    print("开始处理xsd校验失败回执")
    messageType = getTextByTag(tree, "guid")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    errLog = "err_%s.log" % (time.strftime("%Y%m%d"))
    with open(os.path.join(receiveBackDir, errLog), "a+", encoding="utf-8") as f:
        f.write("[%s] " % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")))
        f.write(messageType)
        f.write(": 返回时间：[%s]" % (returnTime))
        f.write(" 原因:[")
        f.write(returnInfo)
        f.write("]\n")

def handleReceipt(root):
    if root.tag.endswith("CEB604Message"):
        handleInvtReceipt(root[0])
    elif root.tag.endswith("CEB506Message"):
        handleLogisticsReceipt(root[0])
    elif root.tag.endswith("CEB304Message"):
        handleOrderReceipt(root[0])
    elif root.tag.endswith("CEB404Message"):
        handlePaymentReceipt(root[0])
    elif root.tag.endswith("CEB900Message"):
        handle900Receipt(root[0])
    elif root.tag.endswith("CEB608Message"):
        handleWayBillReceipt(root[0])
    elif root.tag.endswith("CEB606Message"):
        handleInvtCancelReceipt(root[0])
    elif root.tag.endswith("CEB510Message"):
        handleDepartureReceipt(root[0])
    elif root.tag.endswith("CEB702Message"):
        handleSummaryReceipt(root[0])
    else:
        print("不能识别的回执暂不处理")

def parseXml(fileName):
    if not fileName.endswith(".xml"):
        return
    try:
        tree = ET.parse(os.path.join(receiveDir, fileName))
        root = tree.getroot()
        handleReceipt(root)
    except Exception:
        traceback.print_exc()

    todayDir = time.strftime("%Y%m%d")
    if not os.path.exists(os.path.join(receiveBackDir, todayDir)):
        os.makedirs(os.path.join(receiveBackDir, todayDir))

    shutil.copyfile(os.path.join(receiveDir, fileName), os.path.join(receiveBackDir, todayDir,
                                                                    '%s_%s.xml' % (time.strftime("%Y%m%d%H%M%S"), uuid.uuid1())))
    os.remove(os.path.join(receiveDir, fileName))

def worker():
    print("worker time is : [%s]" % (time.strftime("%Y-%m-%d %H:%M:%S")))
    for parent,dirnames,filenames in os.walk(receiveDir):
       for filename in filenames:
           print("filename is: %s" % (os.path.join(parent, filename)))
           thread_pool.submit(parseXml, filename)
           # parseXml(filename)
    schedule.enter(delay, 0, worker)


if __name__ == "__main__":
    schedule.enter(delay, 0, worker)
    schedule.run()
