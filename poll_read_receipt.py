import uuid, time, os, shutil, sched, pymysql, traceback, datetime
import xml.etree.cElementTree as ET
import operator as op
import queue
import threading
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dbutils.pooled_db import PooledDB

schedule = sched.scheduler(time.time, time.sleep)
delay = int(os.getenv('DELAY', '3'))
messageCount = int(os.getenv('MESSAGE_COUNT', '2000'))
# cache = queue.Queue(maxsize=2000)
cache = Queue(maxsize=messageCount)
receiveBackDir = os.getenv('RECEIVE_BAK_DIR', '/home/zhaopei/data/back')
#receiveBackDir = "/home/zhaopei/data/back"
receiveDir = os.getenv('RECEIVE_DIR', '/home/zhaopei/data/send2')
#receiveDir = "/home/zhaopei/data/send2"
host = os.getenv('HOST', '39.104.185.228')
port = int(os.getenv('PORT', '33306'))
user = os.getenv('USER', 'root')
passord = os.getenv('PASSWORD', 'cargo@yuehai')
databases = os.getenv('DATABASES', 'bills')
maxconnections = int(os.getenv('MAX_CONNECTIONS', '8'))
mincached = int(os.getenv('MIN_CACHED', '4'))
maxcached = int(os.getenv('MAX_CACHED', '4'))

xmlns = "{http://www.chinaport.gov.cn/ceb}"
pool = PooledDB(
creator=pymysql, # 使用链接数据库的模块
maxconnections=maxconnections, # 连接池允许的最大连接数，0和None表示不限制连接数
mincached=mincached, # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
maxcached=maxcached, # 链接池中最多闲置的链接，0和None不限制
maxshared=0, # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
blocking=True, # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
maxusage=None, # 一个链接最多被重复使用的次数，None表示无限制
setsession=[], # 开始会话前执行的命令列表。如：[“set datestyle to …”, “set time zone …”]
ping=1,
# ping MySQL服务端，检查是否服务可用。
# 如：0 = None = never,
# 1 = default = whenever it is requested,
# 2 = when a cursor is created,
# 4 = when a query is executed,
# 7 = always
host=host,
port=port,
user=user,
password=passord,
database=databases,
charset='utf8'
)

thread_pool = ThreadPoolExecutor(4)
process_pool = ProcessPoolExecutor()


def fetchOne(sql, **kw):
    conn = pool.connection()
    cursor = conn.cursor()
    result = None
    try:
        cursor.execute(sql, **kw)
        result = cursor.fetchone()
    except Exception:
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
                
    return result

def fetchAll(sql, **kw):
    conn = pool.connection()
    cursor = conn.cursor()
    result = None
    try:
        cursor.execute(sql, **kw)
        result = cursor.fetchall()
    except Exception:
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    return result

def sqlModify(sql, **kw):
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, **kw)
        conn.commit()
    except Exception:
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def getTextByTag(tree, tagName):
    tagObj = tree.find("%s%s" % (xmlns, tagName))
    if tagObj is None:
        return None
    else:
        return tagObj.text

def handleOrderReceipt(tree):
    print(get_log_prefix(), "开始处理订单回执")
    orderNo = getTextByTag(tree, "orderNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sorder_no, sreturn_status, sreturn_time, sreturn_info, sstatus
    from t_order_head where sorder_no = '%s'
    ''' % (orderNo)
    print(get_log_prefix(), "开始执行：%s" % (sql))

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
            print(get_log_prefix(), "开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if returnStatus == result[4]:
                print(get_log_prefix(), '当前订单状态与回执状态一致，无需更新.')
                return
            if -1 != returnInfo.find('重复发送'):
                print(get_log_prefix(), '订单重复发送! 当前订单状态: [%s]' % (result[4]))
                return
            if -1 != returnInfo.find('处理失败'):
                print(get_log_prefix(), '订单报文处理失败! 当前订单状态: [%s]' % (result[4]))
                return
            if op.lt(result[2], returnTime):
                print(get_log_prefix(), "开始执行：%s" % (sql))
                sqlModify(sql)
            else:
                print(get_log_prefix(), "订单当前状态: [%s], 回执状态: [%s], 无需更新." % (result[4], returnStatus))

def handleLogisticsReceipt(tree):
    print(get_log_prefix(), "开始处理运单回执")
    logisticsNo = getTextByTag(tree, "logisticsNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select slogistics_no, sreturn_status, sreturn_time, sreturn_info, sstatus
    from t_logistics where slogistics_no = '%s'
    ''' % (logisticsNo)
    print(get_log_prefix(), "开始执行：%s" % (sql))

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
            print(get_log_prefix(), "开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if returnStatus == result[4]:
                print(get_log_prefix(), '当前运单状态与回执状态一致，无需更新.')
                return
            if -1 != returnInfo.find('重复发送'):
                print(get_log_prefix(), '运单重复发送! 当前运单状态: [%s]' % (result[4]))
                return
            if -1 != returnInfo.find('处理失败'):
                print(get_log_prefix(), '运单报文处理失败! 当前运单状态: [%s]' % (result[4]))
                return

            if op.lt(result[2], returnTime):
                print(get_log_prefix(), "开始执行：%s" % (sql))
                sqlModify(sql)
            else:
                print(get_log_prefix(), "运单当前状态: [%s], 回执状态: [%s], 无需更新." % (result[4], returnStatus))

def handlePaymentReceipt(tree):
    print(get_log_prefix(), "开始处理收款单回执")
    orderNo = getTextByTag(tree, "orderNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sorder_no, sreturn_status, sreturn_time, sreturn_info, sstatus
    from t_payment where sorder_no = '%s'
    ''' % (orderNo)
    print(get_log_prefix(), "开始执行：%s" % (sql))

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
            print(get_log_prefix(), "开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if returnStatus == result[4]:
                print(get_log_prefix(), '当前收款单状态与回执状态一致，无需更新.')
                return
            if -1 != returnInfo.find('重复发送'):
                print(get_log_prefix(), '收款单重复发送! 当前收款单状态: [%s]' % (result[4]))
                return
            if -1 != returnInfo.find('处理失败'):
                print(get_log_prefix(), '收款单报文处理失败! 当前收款单状态: [%s]' % (result[4]))
                return
            if op.lt(result[2], returnTime):
                print(get_log_prefix(), "开始执行：%s" % (sql))
                sqlModify(sql)
            else:
                print(get_log_prefix(), "收款单当前状态: [%s], 回执状态: [%s], 无需更新." % (result[4], returnStatus))

def handleInvtReceipt(tree):
    print(get_log_prefix(), "开始处理清单回执")
    orderNo = getTextByTag(tree, "orderNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sorder_no, sreturn_status, sreturn_time, sreturn_info, sstatus
    from t_invt_head where sorder_no = '%s'
    ''' % (orderNo)
    print(get_log_prefix(), "开始执行：%s" % (sql))

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
            print(get_log_prefix(), "开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if returnStatus == result[4]:
                print(get_log_prefix(), '当前清单状态与回执状态一致，无需更新.')
                return
            if -1 != returnInfo.find('清单数据已存在'):
                print(get_log_prefix(), "清单数据已存在，忽略回执! 当前清单状态: [%s]" % (result[4]))
                return
            if op.lt(result[2], returnTime):
                print(get_log_prefix(), "开始执行：%s" % (sql))
                sqlModify(sql)
            else:
                print(get_log_prefix(), "清单当前状态: [%s], 回执状态: [%s], 无需更新." % (result[4], returnStatus))

def handleInvtCancelReceipt(tree):
    print(get_log_prefix(), "开始处理撤销清单回执")
    invtNo = getTextByTag(tree, "invtNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sinvt_no, sreturn_status, sreturn_time, sreturn_info, sstatus
    from t_invt_cancel where sinvt_no = '%s'
    ''' % (invtNo)
    print(get_log_prefix(), "开始执行：%s" % (sql))

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
            print(get_log_prefix(), "开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if returnStatus == result[4]:
                print(get_log_prefix(), '当前撤销申请单状态与回执状态一致，无需更新.')
                return
            if op.lt(result[2], returnTime):
                print(get_log_prefix(), "开始执行：%s" % (sql))
                sqlModify(sql)
            else:
                print(get_log_prefix(), "撤销申请单当前状态: [%s], 回执状态: [%s], 无需更新." % (result[4], returnStatus))

def handleWayBillReceipt(tree):
    print(get_log_prefix(), "开始处理清单总分单回执")
    billNo = getTextByTag(tree, "billNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sbill_no, sreturn_status, sreturn_time, sreturn_info, sstatus
    from t_waybill_head where sbill_no = '%s'
    ''' % (billNo)
    print(get_log_prefix(), "开始执行：%s" % (sql))

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
            print(get_log_prefix(), "开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if returnStatus == result[4]:
                print(get_log_prefix(), '当前总分单状态与回执状态一致，无需更新.')
                return
            if -1 != returnInfo.find('已存在'):
                print(get_log_prefix(), "总分单数据已存在，忽略回执! 当前总分单状态: [%s]" % (result[4]))
                return
            if op.lt(result[2], returnTime):
                print(get_log_prefix(), "开始执行：%s" % (sql))
                sqlModify(sql)
            else:
                print(get_log_prefix(), "总分单当前状态: [%s], 回执状态: [%s], 无需更新." % (result[4], returnStatus))

def handleDepartureReceipt(tree):
    print(get_log_prefix(), "开始离境单回执")
    copNo = getTextByTag(tree, "copNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sbill_no, sreturn_status, sreturn_time, sreturn_info, sstatus
    from t_departure_head where scop_no = '%s'
    ''' % (copNo)
    print(get_log_prefix(), "开始执行：%s" % (sql))

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
            print(get_log_prefix(), "开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if returnStatus == result[4]:
                print(get_log_prefix(), '当前离境单状态与回执状态一致，无需更新.')
                return
            if op.lt(result[2], returnTime):
                print(get_log_prefix(), "开始执行：%s" % (sql))
                sqlModify(sql)
            else:
                print(get_log_prefix(), "离境单当前状态: [%s], 回执状态: [%s], 无需更新." % (result[4], returnStatus))

def handleSummaryReceipt(tree):
    print(get_log_prefix(), "开始处理汇总单回执")
    copNo = getTextByTag(tree, "copNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sreturn_status, sreturn_time, sreturn_info, sstatus
    from t_summary_head where scop_no = '%s'
    ''' % (copNo)
    print(get_log_prefix(), "开始执行：%s" % (sql))

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
            print(get_log_prefix(), "开始执行：%s" % (sql))
            sqlModify(sql)
        else:
            if returnStatus == result[4]:
                print(get_log_prefix(), '当前汇总申请单状态与回执状态一致，无需更新.')
                return
            if op.lt(result[1], returnTime):
                print(get_log_prefix(), "开始执行：%s" % (sql))
                sqlModify(sql)
            else:
                print(get_log_prefix(), "汇总申请单当前状态: [%s], 回执状态: [%s], 无需更新." % (result[3], returnStatus))

def handle900Receipt(tree):
    print(get_log_prefix(), "开始处理xsd校验失败回执")
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

def handleReceipt():
    root = cache.get()
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
        print(get_log_prefix(), "不能识别的回执暂不处理")

def parseXml(fileName):
    if not fileName.endswith(".xml"):
        return
    try:
        tree = ET.parse(os.path.join(receiveDir, fileName))
        root = tree.getroot()
        cache.put(root)
        # handleReceipt(root)
        # thread_pool.submit(handleReceipt)
        process_pool.submit(handleReceipt)
    except Exception:
        traceback.print_exc()

    todayDir = time.strftime("%Y%m%d")
    if not os.path.exists(os.path.join(receiveBackDir, todayDir)):
        os.makedirs(os.path.join(receiveBackDir, todayDir))

    shutil.copyfile(os.path.join(receiveDir, fileName), os.path.join(receiveBackDir, todayDir,
                                                                    '%s_%s.xml' % (time.strftime("%Y%m%d%H%M%S"), uuid.uuid1())))
    os.remove(os.path.join(receiveDir, fileName))

def worker():
    print(get_log_prefix(), "worker time is : [%s]" % (time.strftime("%Y-%m-%d %H:%M:%S")))
    for parent,dirnames,filenames in os.walk(receiveDir):
       for filename in filenames:
           print(get_log_prefix(), "filename is: %s" % (os.path.join(parent, filename)))
           if cache.full():
               break
           parseXml(filename)
    schedule.enter(delay, 0, worker)

def get_log_prefix():
    threading_name = threading.current_thread().name
    return '[{}] [{}] [{}] '.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), os.getpid(), threading_name)

if __name__ == "__main__":
    schedule.enter(delay, 0, worker)
    schedule.run()
