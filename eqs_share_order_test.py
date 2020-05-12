# coding=utf-8
import mysql.connector
import tracemalloc

columns = [
    # 'id',
    'order_type',
    'app_id',
    'order_no',
    'cash_type',
    'user_id',
    'total_cash',
    'should_share_cash',
    'actual_share_cash',
    'create_time',
    'plat_share_user_id',
    'plat_should_share_cash',
    'plat_actual_share_cash',
    'start_time',
    'end_time',
    'share_time',
    'status',
    'goods_code',
    'goods_name',
    'template_id',
    'remark',
    'xd_log_id',
    'share_login_id',
]
table_name = 'eqs_share_order_test'
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '123456',
    'database': 'test',
}


def init_sync():
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    sql = "select {0} from {1}".format(','.join(columns), table_name)
    cursor.execute(sql)

    tracemalloc.start()
    snapshot1 = tracemalloc.take_snapshot()
    rows = cursor.fetchall()
    for n in range(40):
        do_insert(rows)
    snapshot2 = tracemalloc.take_snapshot()
    top_stats = snapshot2.compare_to(snapshot1, 'lineno')

    print("[ Top 10 differences ]")
    for stat in top_stats[:10]:
        print(stat)


def do_insert(rows):
    cs = ','.join(columns)
    vs = ','.join(['%s' for i in range(len(columns))])
    sql = 'insert into eqs_share_order_test_dist ( {0} ) values ({1}) '.format(cs, vs)

    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    cursor.executemany(sql, rows)
    cnx.commit()

    cursor.close()
    cnx.close()


if __name__ == '__main__':
    init_sync()
