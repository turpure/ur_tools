#! usr/bin/python
# coding:utf8

import csv
from db_connection import MsSQL

db = MsSQL()


def split_images():
    sql = ("select SKU,extra_images from oa_goodsinfo as ofo "
            "LEFT JOIN oa_wishgoods as ogs on ofo.pid = ogs.infoid "
            "where completeStatus like '%wish%'")
    with db as con:
        cur = con.cursor(as_dict=True)
        cur.execute(sql)
        outs = cur.fetchall()
        for ret in outs:
            target = [(ret['SKU'])]
            images = ret['extra_images'].split('\n')
            target.extend(images)
            yield target


def export_csv(rows):
    """
    :param rows:
    :return:
    """
    with open('D:\wish_images.csv', 'wb') as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


if __name__ == "__main__":
    images = split_images()
    export_csv(images)