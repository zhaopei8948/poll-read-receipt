import uuid, sys, time, codecs, os, shutil, sched, pymysql, traceback, datetime
import xml.etree.cElementTree as ET
import operator as op


schedule = sched.scheduler(time.time, time.sleep)
delay = 3
receiveBackDir = r"/Users/zhaopei/Desktop/5/pdata/2"
receiveDir = r"/Users/zhaopei/Desktop/5/pdata/1"
xmlns = "{http://www.chinaport.gov.cn/ceb}"
host, userName, password, dbs = "localhost", "root", "root", "bills"
cursor = None

def dbOpenClose(func):
    def wrapper(*args, **kw):
        global cursor
        db = pymysql.connect(host, userName, password, dbs);
        cursor = db.cursor()
        output = None
        try:
            output = func(*args, **kw)
            db.commit()
        except Exception as e:
            traceback.print_exc()
            db.rollback()
        finally:
            cursor.close()
            db.close()
        return output
    return wrapper

def getTextByTag(tree, tagName):
    tagObj = tree.find("%s%s" % (xmlns, tagName))
    if tagObj is None:
        return None
    else:
        return tagObj.text

@dbOpenClose
def handleOrderReceipt(tree):
    print("开始处理订单回执")
    orderNo = getTextByTag(tree, "orderNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sorder_no sreturn_status, sreturn_time, sreturn_info
    from t_order_head where sorder_no = '%s'
    ''' % (orderNo)
    print("开始执行：%s" % (sql))

    cursor.execute(sql)
    result = cursor.fetchone()

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
            cursor.execute(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                cursor.execute(sql)

@dbOpenClose
def handleLogisticsReceipt(tree):
    print("开始处理运单回执")
    logisticsNo = getTextByTag(tree, "logisticsNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select slogistics_no sreturn_status, sreturn_time, sreturn_info
    from t_logistics where slogistics_no = '%s'
    ''' % (logisticsNo)
    print("开始执行：%s" % (sql))

    cursor.execute(sql)
    result = cursor.fetchone()

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
            cursor.execute(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                cursor.execute(sql)

@dbOpenClose
def handlePaymentReceipt(tree):
    print("开始处理收款单回执")
    orderNo = getTextByTag(tree, "orderNo")
    returnStatus = getTextByTag(tree, "returnStatus")
    returnTime = getTextByTag(tree, "returnTime")
    returnInfo = getTextByTag(tree, "returnInfo")
    sql = '''
    select sorder_no sreturn_status, sreturn_time, sreturn_info
    from t_payment where sorder_no = '%s'
    ''' % (orderNo)
    print("开始执行：%s" % (sql))

    cursor.execute(sql)
    result = cursor.fetchone()

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
            cursor.execute(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                cursor.execute(sql)

@dbOpenClose
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

    cursor.execute(sql)
    result = cursor.fetchone()

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
            cursor.execute(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                cursor.execute(sql)

@dbOpenClose
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

    cursor.execute(sql)
    result = cursor.fetchone()

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
            cursor.execute(sql)
        else:
            if op.lt(result[2], returnTime):
                print("开始执行：%s" % (sql))
                cursor.execute(sql)

def handle900Receipt(tree):
    print("开始处理xsd校验失败回执")
    messageType = getTextByTag(tree, "guid")
    returnStatus = getTextByTag(tree, "returnStatus")
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
    else:
        print("不是四单回执暂不处理")

def parseXml(fileName):
    if not fileName.endswith(".xml"):
        return
    tree = ET.parse(os.path.join(receiveDir, fileName))
    root = tree.getroot()
    # subtree = root[0]
    # handleInvtReceipt(subtree)
    handleReceipt(root)
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
           parseXml(filename)
    schedule.enter(delay, 0, worker)


if __name__ == "__main__":
    schedule.enter(delay, 0, worker)
    schedule.run()
