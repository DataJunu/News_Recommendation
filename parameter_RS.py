"""
20180705

계산된 이웃을 통한 뉴스 추천

"""





import pandas as pd
import pymysql

def sim_distance(prefs, person1, person2):
    si = {}

    for item in prefs[person1]:
        if item in prefs[person2]: si[item] = 1

    if len(si) == 0: return None

    score = sum([1.0 for item in prefs[person1] if item in prefs[person2]])

    return score
def topMatches(prefs, person,similarity=sim_distance):
    scores = [(similarity(prefs,person,other), other)
              for other in prefs if other != person]
    # 스코어 없는 애들 배제하기 위함
    scores = [(sr,idx) for sr,idx in scores if sr is not None]
    scores.sort()
    scores.reverse()
    return scores[0:300]


conn = pymysql.connect(host='localhost', user='root', password='', db='temp', charset='utf8',
                       cursorclass=pymysql.cursors.DictCursor)
curs = conn.cursor(pymysql.cursors.DictCursor)


user_query = "select user_id,concat(날짜,\"-\",시퀀스번호,\"-\",언론사코드) as news_code,시간 from news where 날짜=" + str(
    20180625)
qr2 = pd.read_sql_query(sql=user_query, con=conn)


user_list = list(set([user_id for user_id in qr2.user_id])) # 테스트할 유저 리스트를 만드는 것

neighbor_list = [20180619]

from ast import literal_eval

for n_day in neighbor_list:

    total_cnt = 0.0
    score_cnt5 = 0.0
    score_cnt10 = 0.0
    score_cnt15 = 0.0
    score_cnt20 = 0.0
    total_users = []
    target_users = []

    # 이웃 소환
    sql = "SELECT * from temp.recent_neighbors3 where date = " + str(n_day)
    neighbor_qry = pd.read_sql_query(sql=sql, con=conn)
    for user in user_list:
        Reco_list5 = []
        Reco_list10 = []
        Reco_list15 = []
        Reco_list20 = []

        # %를 측정하기 위한 변수
        total_cnt += 1.0



        #total_users.append(user)
        try:
            # 이웃 검색
            nbr = neighbor_qry[neighbor_qry.user_id == user].neighbors
            # 리스트 형태로 디비에 저장되있었던 것을 파이썬의 리스트 형태로 재 로드 ( 라이브러리 써야 됨)
            tup = list(literal_eval(nbr.tolist()[0]))
            # 판다스 형식 통일을 위하여 데이터 프레임으로 변경
            nbr = pd.DataFrame(tup, columns=['score', 'user_id'])
        except:
            continue

        # 사용자의 처음 조회 시간을 알기 위한 index
        index_list = qr2[qr2.user_id == user].sort_values(by=['시간']).index.tolist()

        # 내 이웃이 추천 직전에 가장 많이 본 뉴스
        recent_time_cdn = qr2.시간 <= int(qr2[qr2.index == index_list[0]].시간)
        data = qr2[recent_time_cdn]

        id_list = nbr.user_id.tolist()
        join_table = data[data['user_id'].isin(id_list)].join(nbr.set_index('user_id'), on='user_id')
        freq_news = join_table[['news_code', 'score']].groupby(['news_code']).agg(
            'sum').sort_values(by=['score'], ascending=False)

        freq_news['news_code'] = freq_news.index
        freq_news.columns = ['score', 'news_code']


        for idx, data in enumerate(freq_news.itertuples()):
            if idx < 5:
                Reco_list5.append(data[2])
            if idx < 10:
                Reco_list10.append(data[2])
            if idx < 15:
                Reco_list15.append(data[2])
            if idx < 20:
                Reco_list20.append(data[2])

        # pandas dataframe에서 해당 user_id의 중간 시간 이후의 뉴스들을 찾아낸다
        user_cdn = qr2['user_id'] == user
        # qr1에 있는 news_code가 topk 안에 들어있는 것 중에 하나라도 겹치면 true

        if any(elem in Reco_list5 for elem in qr2[user_cdn]['news_code'].values):
            target_users.append(user)
            score_cnt5 += 1.0
        if any(elem in Reco_list10 for elem in qr2[user_cdn]['news_code'].values):
            score_cnt10 += 1.0
        if any(elem in Reco_list15 for elem in qr2[user_cdn]['news_code'].values):
            score_cnt15 += 1.0
        if any(elem in Reco_list20 for elem in qr2[user_cdn]['news_code'].values):
            score_cnt20 += 1.0

    print("해당 날짜: ", n_day, "날짜까지의 이웃을 썼을 때의 성능 평가==================================================================")
    print("Top - 5 성능 지표 :", total_cnt, "명 중에 ", score_cnt5, "명은 해당 추천된 뉴스를 보았습니다.")
    print(score_cnt5 / total_cnt * 100.0, "% ")
    print('=========================================')

    print("Top - 10 성능 지표 :", total_cnt, "명 중에 ", score_cnt10, "명은 해당 추천된 뉴스를 보았습니다.")
    print(score_cnt10 / total_cnt * 100.0, "% ")
    print('=========================================')

    print("Top - 15 성능 지표 :", total_cnt, "명 중에 ", score_cnt15, "명은 해당 추천된 뉴스를 보았습니다.")
    print(score_cnt15 / total_cnt * 100.0, "% ")
    print('=========================================')

    print("Top - 20 성능 지표 :", total_cnt, "명 중에 ", score_cnt20, "명은 해당 추천된 뉴스를 보았습니다.")
    print(score_cnt20 / total_cnt * 100.0, "% ")
    print('=========================================')
    #with open('data_folder/target_user.pickle', 'wb') as mysavedata1:
    #    pickle.dump(target_users, mysavedata1)
    #with open('data_folder/total_user.pickle', 'wb') as mysavedata2:
    #    pickle.dump(total_users, mysavedata2)
