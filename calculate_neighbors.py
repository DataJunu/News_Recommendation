"""

2018 07 05
실제 시스템에 올라가기 위해서
사전에 이웃을 구해놓는 코드

매트릭스 연산을 통해 자카르트 계수를 계산함

"""


from __future__ import division

import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from sklearn import preprocessing


def topMatches(prefs, person,R,le2):
    scores = []

    # person의 상위 이웃 (역sort)
    neighbor_idx = np.argsort(R.tolist()[0])[::-1][:300]

    #사용자 아이디와 점수를 scores 리스트에 저장
    for user, score in zip(le2.inverse_transform(neighbor_idx), R[0, neighbor_idx].tolist()[0]):
        if score >0 and person != user:
            scores.append((score, user))
    return scores


# user by item 매트릭스가 들어왔을 때, user by user 유사도 매트릭스를 구해주는 함수
def pairwise_jaccard(X):
    """Computes the Jaccard distance between the rows of `X`.
    """
    X = X.astype(bool).astype(int)
    intrsct = X.dot(X.T)
    row_sums = intrsct.diagonal()
    unions = row_sums[:,None] + row_sums - intrsct
    dist = intrsct / unions
    return dist


def insert_nbr(day,curs,conn):

    #sql = "SELECT user_id,concat(날짜,\"-\",시퀀스번호,\"-\",언론사코드) as news_code,시간 FROM temp.news where 날짜 >= " + str(
    #    day) +" and 날짜 like '201806%' and 날짜 !=20180625"
    sql = "SELECT user_id,concat(날짜,\"-\",시퀀스번호,\"-\",언론사코드) as news_code,시간 FROM temp.news where 날짜 >= " + str(
            day) +" and 날짜 !=20180625"
    qr1 = pd.read_sql_query(sql=sql, con=conn)


########################################################################################################################
    start_time = time.time()
    print("start_time", start_time)  # 출력해보면, 시간형식이 사람이 읽기 힘든 일련번호형식입니다.

    # item encoder
    le = preprocessing.LabelEncoder()
    le.fit(qr1['news_code'])
    qr1['item_id'] = le.transform(qr1['news_code'])
    qr1['rating'] = 1

    # user encoder
    le2 = preprocessing.LabelEncoder()
    le2.fit(qr1['user_id'])
    qr1['u_id'] = le2.transform(qr1['user_id'])

    qr1 = qr1[['u_id', 'item_id', 'rating' , 'user_id']]

    mtx = csr_matrix((qr1.rating,(qr1.u_id,qr1.item_id)))
    sim_mtx = pairwise_jaccard(mtx)
    print("matrix 만드는 시간 %s" % (time.time() - start_time))
########################################################################################################################

    sql = """insert into recent_neighbors3(date,user_id,neighbors)
                     values (%s, %s, %r)"""

    for uid in qr1.u_id.unique():

        try:
            user = le2.inverse_transform(uid)
            nbr = topMatches(qr1,user,sim_mtx[uid],le2)
            curs.execute(sql, (day,user,nbr))
            conn.commit()

        except Exception as error:
            print (error)
            continue

import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', db='temp', charset='utf8', )

curs = conn.cursor(pymysql.cursors.DictCursor)
days = [20180416]

import time

for day in days:
    start_time = time.time()
    print(day)
    insert_nbr(day,curs,conn)
    print("start_time", start_time)  # 출력해보면, 시간형식이 사람이 읽기 힘든 일련번호형식입니다.
    print("--- %s seconds ---" % (time.time() - start_time))


conn.close()
