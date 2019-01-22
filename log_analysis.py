#!/usr/bin/env python

import psycopg2


def question_1(cursor):
    cursor.execute('''
    SELECT a.title, (string_to_array(path,'/'))[3] as article, count(*) as c
    FROM log as l JOIN articles as a on a.slug=(string_to_array(path,'/'))[3]
    WHERE (string_to_array(path,'/'))[3] is not null
    GROUP BY article, a.title ORDER BY c desc;''')
    results = cursor.fetchall()
    print("-" * 80)
    for r in results:
        print('"{}" - {} views'.format(r[0], r[2]))


def question_2(cursor):
    sql_query = '''
    SELECT au.name, count(*) as c
    FROM log as l JOIN articles as a on a.slug=(string_to_array(path,'/'))[3]
    JOIN authors as au ON a.author=au.id
    WHERE (string_to_array(path,'/'))[3] is not null
    GROUP BY au.id, au.name
    ORDER BY c desc;
'''
    cursor.execute(sql_query)
    results = cursor.fetchall()
    print("-" * 80)
    for r in results:
        print('{} - {} views'.format(r[0], r[1]))


def question_3(cursor):
    sql_query = '''
    select t1.day, CAST (t2.error_views as FLOAT) / t1.views as rate
    FROM ( SELECT date_trunc('day', time) as day, count(*) as views
    from log group by day ) as t1
    JOIN ( SELECT date_trunc('day', time) as day, count(*) as error_views
    from log where CAST((string_to_array(status,' '))[1] AS INTEGER) >=400
    group by day ) as t2 on t1.day = t2.day
    WHERE CAST (t2.error_views as FLOAT) / t1.views > 0.01
    '''
    cursor.execute(sql_query)
    results = cursor.fetchall()
    print("-" * 80)
    for r in results:
        print('{} - {:.2%} errors'.format(r[0].date(), r[1]))


def main():
    conn = psycopg2.connect('dbname=news')
    cursor = conn.cursor()
    question_1(cursor)
    question_2(cursor)
    question_3(cursor)


if __name__ == "__main__":
    main()
