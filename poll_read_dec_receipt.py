import uuid, time, os, shutil, sched
import pymysql
import traceback
import operator
import xml.etree.cElementTree as ET
from loguru import logger

schedule = sched.scheduler(time.time, time.sleep)
delay = int(os.getenv('DELAY') or 3)
receiveBackDir = os.getenv('RECEIVE_BACK_DIR') or r"/Users/zhaopei/Desktop/5/pdata/2"
receiveDir = os.getenv('RECEIVE_DIR') or r"/Users/zhaopei/Desktop/5/pdata/1"
xmlns = os.getenv('XMLNS') or "{http://www.chinaport.gov.cn/dec}"
host = os.getenv('MYSQL_HOST') or 'localhost'
userName = os.getenv('MYSQL_USERNAME') or 'root'
password = os.getenv('MYSQL_PASSWORD') or 'root'
dbs = os.getenv('MYSQL_DB') or 'ymt_order'
port = int(os.getenv('MYSQL_PORT') or 3306)
cursor = None
# logger.add(sys.stdout, colorize=True, format="<green>{time}</green> <level>{message}</level>")

def dbOpenClose(func):
    def wrapper(*args, **kw):
        global cursor
        db = pymysql.connect(host, userName, password, dbs, port=port);
        cursor = db.cursor()
        logger.info("cursor={}".format(cursor))
        output = None
        try:
            output = func(*args, **kw)
            db.commit()
        except Exception:
            traceback.print_exc()
            db.rollback()
        finally:
            cursor.close()
            db.close()
        return output
    return wrapper

def getTextByTag(tree, tagName):
    tagObj = tree.find(tagName)
    if tagObj is None:
        tagObj = tree.find("{}{}".format(xmlns, tagName))
    # logger.info("tree: {}, tagName: {}, find tag is {}".format(tree, tagName, tagObj))
    if tagObj is None:
        return None
    else:
        return tagObj.text

@dbOpenClose
def handleDecImportResponse(tree, content):
    logger.info("开始处理报关单导入回执")
    for child in tree:
        logger.info("child is: {}, text: {}".format(child.tag, child.text))
    responseCode = getTextByTag(tree, "ResponseCode")
    errorMessage = getTextByTag(tree, "ErrorMessage")
    clientSeqNo = getTextByTag(tree, "ClientSeqNo")
    seqNo = getTextByTag(tree, "SeqNo")
    trnPreId = getTextByTag(tree, "TrnPreId")
    print(responseCode)
    logger.info("responseCode={}, errorMessage={}, clientSeqNo={}, seqNo={}, trnPreid={}".format(
        responseCode, errorMessage, clientSeqNo, seqNo, trnPreId
    ))
    sql = 'select dec_guid from t_dec_sign where client_seq_no = %s'
    logger.info("execute sql: {}".format(sql))
    cursor.execute(sql, [clientSeqNo])
    guid = cursor.fetchone()
    if guid is None:
        logger.error('clientSeqNo {} 不存在，不需要更新数据'.format(clientSeqNo))
        return
    guid = guid[0]
    sql = '''
    update t_dec_head t set t.dec_status = %s, t.last_note = %s, t.seq_no = %s
    where t.guid = %s
    '''
    if operator.eq("0", responseCode):
        decStatus = "0"
    else:
        decStatus = "-1"
    logger.info("execute sql: {}".format(sql))
    cursor.execute(sql, [decStatus, errorMessage, guid, seqNo])

    sql = '''
    insert into t_dec_receipt (guid, dec_guid, channel, note, notice_date, input_time, content)
    values (%s, %s, %s, %s, str_to_date(%s, '%Y%m%d%H%i%s'),
    str_to_date(%s, '%Y%m%d%H%i%s'), %s
    )
    '''
    logger.info("execute sql: {}".format(sql))
    cursor.execute(sql, [uuid.uuid1(), guid, decStatus, errorMessage, time.strftime("%Y%m%d%H%M%S"),
                         time.strftime("%Y%m%d%H%M%S"), content])


@dbOpenClose
def handleDecData(tree, content):
    logger.info("开始处理报关回执")
    for child in tree:
        logger.info("child is: {}".format(child.tag))
    result = tree.find("DEC_RESULT") or tree.find("%s%s" % (xmlns, "DEC_RESULT"))
    for child in result:
        logger.info("result child is: {}, text is: {}".format(child.tag, child.text))
    resultInfo = getTextByTag(tree, "RESULT_INFO")
    seqNo = getTextByTag(result, "SEQ_NO")
    entryId = getTextByTag(result, "ENTRY_ID")
    noticeDate = getTextByTag(result, "NOTICE_DATE")
    if noticeDate is None:
        noticeDate = time.strftime("%Y%m%d%H%M%S")
    else:
        noticeDate = noticeDate.replace('T', '')
    channel = getTextByTag(result, "CHANNEL")
    note = getTextByTag(result, "NOTE")
    declPort = getTextByTag(result, "DECL_PORT")
    agentName = getTextByTag(result, "AGENT_NAME")
    declareNo = getTextByTag(result, "DECLARE_NO")
    tradeCo = getTextByTag(result, "TRADE_CO")
    customsField = getTextByTag(result, "CUSTOMS_FIELD")
    bondedNo = getTextByTag(result, "BONDED_NO")
    ieDate = getTextByTag(result, "I_E_DATE")
    packNo = getTextByTag(result, "PACK_NO")
    billNo = getTextByTag(result, "BILL_NO")
    trafMode = getTextByTag(result, "TRAF_MODE")
    voyageNo = getTextByTag(result, "VOYAGE_NO")
    netWt = getTextByTag(result, "NET_WT")
    grossWt = getTextByTag(result, "GROSS_WT")
    dDate = getTextByTag(result, "D_DATE")
    logger.info('''
                seqNo={}, entryId={}, noticeDate={}, channel={}, note={},
                declPort={}, agentName={}, declareNo={},
                tradeCo={}, customsField={}, bondedNo={},
                ieDate={}, packNo={}, billNo={}, trafMode={},
                voyageNo={}, netWt={}, grossWt={}, dDate={}, resultInfo={}
                '''.format(
                    seqNo, entryId, noticeDate, channel, note, declPort, agentName, declareNo,
                    tradeCo, customsField, bondedNo, ieDate, packNo, billNo, trafMode,
                    voyageNo, netWt, grossWt, dDate, resultInfo
    ))

    sql = 'select guid from t_dec_head where seq_no = %s'
    logger.info("execute sql: {}".format(sql))
    cursor.execute(sql, [seqNo])
    guid = cursor.fetchone()
    if guid is None:
        logger.error('seqNo {} 不存在，不需要更新数据'.format(seqNo))
        return
    guid = guid[0]

    sql = '''
    update t_dec_head t set t.dec_status = :decStatus, t.last_note = :lastNote, t.last_result_info = :lastResultInfo
    , t.last_receipt_time = str_to_date(:lastReceiptTime, '%Y%m%d%H%i%s')
    where t.seq_no = :seqNo
    '''
    cursor.prepare(sql)
    logger.info("execute sql: {}".format(sql))
    cursor.execute(None, decStatus=channel, lastNote=note, lastResultInfo=resultInfo,
                   lastReceiptTime=noticeDate, seqNo=seqNo)

    sql = 'select guid from t_dec_head where seq_no = :seqNo'
    cursor.prepare(sql)
    logger.info("execute sql: {}".format(sql))
    cursor.execute(None, seqNo=seqNo)
    guid = cursor.fetchone()
    if guid is None:
        logger.error('seqNo {} 不存在，不需要更新数据'.format(seqNo))
        return
    guid = guid[0]

    sql = '''
    insert into t_dec_receipt (guid, dec_guid, channel, note, notice_date, input_time, content)
    values (:guid, :decGuid, :channel, :note, str_to_date(:noticeDate, '%Y%m%d%H%i%s'),
    str_to_date(:inputTime, '%Y%m%d%H%i%s'), :content
    )
    '''
    cursor.prepare(sql)
    logger.info("execute sql: {}".format(sql))
    cursor.execute(None, guid=uuid.uuid1(), decGuid=guid, channel=channel, note=note,
                   noticeDate=noticeDate, inputTime=time.strftime("%Y%m%d%H%M%S"),
                   content=content)


def handleReceipt(root, content):
    if root.tag.endswith("DecImportResponse"):
        handleDecImportResponse(root, content)
    elif root.tag.endswith("DEC_DATA"):
        handleDecData(root, content)
    else:
        print("回执格式不对，暂时不处理")

def parseXml(fileName):
    if not fileName.endswith(".xml"):
        return
    prefix = ""
    try:
        tree = ET.parse(os.path.join(receiveDir, fileName))
        content = ""
        with open(os.path.join(receiveDir, fileName)) as f:
            content = f.read()
        logger.info("xml is {}".format(content))
        root = tree.getroot()
        handleReceipt(root, content)
        prefix = "SUCCESS_"
    except Exception:
        traceback.print_exc()
        prefix = "ERROR_"
    todayDir = time.strftime("%Y%m%d")
    if not os.path.exists(os.path.join(receiveBackDir, todayDir)):
        os.makedirs(os.path.join(receiveBackDir, todayDir))

    shutil.copyfile(os.path.join(receiveDir, fileName), os.path.join(receiveBackDir, todayDir,
                                                                    '%s%s_%s.xml' % (prefix, time.strftime("%Y%m%d%H%M%S"), uuid.uuid1())))
    os.remove(os.path.join(receiveDir, fileName))

def worker():
    logger.info("worker time is : [%s]" % (time.strftime("%Y-%m-%d %H:%M:%S")))
    for parent,dirnames,filenames in os.walk(receiveDir):
       for filename in filenames:
           logger.info("filename is: %s" % (os.path.join(parent, filename)))
           parseXml(filename)
    schedule.enter(delay, 0, worker)


if __name__ == "__main__":
    schedule.enter(delay, 0, worker)
    schedule.run()
