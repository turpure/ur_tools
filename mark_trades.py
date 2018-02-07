#! usr/bin/env/python
# coding=utf8

import datetime
import re


from db_connection import MsSQL
from log import logger

db = MsSQL()


def filter_trade():
    """ filter of trade via sku statutes """
    filter_procedure = "www_outofstock_sku '7','春节放假,清仓,停产,停售,线下清仓'"
    update_memo = "update p_tradeUn set memo = %s, reasonCode = %s where nid = %s"
    empty_mark = "update p_tradeUn set reasonCode = '', memo = %s where nid = %s"
    today = str(datetime.datetime.now())[5:10]
    pattern = u'不采购: .*;'
    mark_trades = dict()  # {nid:[mark_memo,memo],}
    with db as con:
        cur = con.cursor(as_dict=True)
        cur.execute(filter_procedure)
        filter_trades = cur
        for tra in filter_trades:
            memo = tra['memo']
            origin_memo = re.sub(unicode(pattern), '', memo)
            if tra['which'] == 'pre':
                cur.execute(empty_mark, (origin_memo, tra['tradeNid']))
                con.commit()
                logger.info('emptying %s', tra['tradeNid'])
            else:
                mark_memo = u'不采购: ' + tra['purchaser'] + today + ':' + tra['sku'] + tra['goodsSkuStatus'] + ';'
                trade = {
                    'tradeNid': tra['tradeNid'],
                    'mark_memo': mark_memo,
                    'origin_memo': origin_memo,
                    'reasonCode': tra['howPur']
                }
                if tra['tradeNid'] in mark_trades:
                    mark_trades[tra['tradeNid']]['mark_memo'] += mark_memo
                else:
                    mark_trades[tra['tradeNid']] = trade
        for mar in mark_trades.values():
            new_memo = mar['origin_memo'] + mar['mark_memo']
            cur.execute(update_memo, (new_memo, mar['reasonCode'], mar['tradeNid']))
            con.commit()
            logger.info('marking %s', tra['tradeNid'])


def handle_exception_trades():
    """
    if delay days of the trade is equal or greater than
    7 and being marked days is equal or greater than 3,
    then the trade should be transported to exception trades
    """
    trans_trade = "select nid,reasoncode,memo,DATEDIFF(day, dateadd(hour,8,ordertime), GETDATE())" \
          " as deltaday from p_tradeun " \
          "where (reasoncode like '%不采购%' or reasoncode like '%春节%') " \
          "and PROTECTIONELIGIBILITYTYPE='缺货订单' " \
          "and DATEDIFF(day, dateadd(hour,8,ordertime), GETDATE())>=7"

    max_bill_code_query = "P_S_CodeRuleGet 130,''"
    exception_trade_handler = "P_ExceptionTradeToException %s, 3 ,'取消订单', '%s'"

    with db as con:
        cur = con.cursor(as_dict=True)
        try:
            cur.execute(trans_trade)
            trades = cur
            for row in trades:
                marked_days = calculate_mark_day(row['memo'])
                if marked_days >= 3:
                    try:
                        cur.execute(max_bill_code_query)
                        max_bill_code = cur.fetchone()['MaxBillCode']
                        cur.execute(exception_trade_handler % (str(row['nid']), str(max_bill_code)))
                        con.commit()
                        logger.info('transporting %s' % row['nid'])
                    except Exception as e:
                        logger.error('%s while transporting %s' % (e, row['nid']))
        except Exception as e:
            logger.error('%s while fetching the exception trades' % e)


def calculate_mark_day(memo):
    try:
        year = str(datetime.datetime.now())[:5]
        lasted_marked_day = (year + re.findall('\d\d-\d\d', memo)[-1]).split('-')
        mark_day = datetime.datetime(int(lasted_marked_day[0]),
                                     int(lasted_marked_day[1]),
                                     int(lasted_marked_day[2]))

    except Exception as e:
        logger.error('%s while calculate the marked day' % e)
        mark_day = datetime.datetime.now()
    today = datetime.datetime.now()
    delta_day = (today-mark_day).days
    if delta_day < 0:
        delta_day = 3
    return delta_day

if __name__ == '__main__':
    filter_trade()

