import pandas as pd
from IPython.display  import display
import requests
from mysql.connector import connect


kuber={'host':"",
       'port':"80",
       'api_key':""}
local={'host':"localhost",
       'port':"1488",
       'api_key':""}
local_py={'host':"localhost",
       'port':"5000",
       'api_key':""}
prod={'host':"",
       'port':"80",
       'api_key':""}

device_id = ''
user_id = '35'

def mysql(query):
    """Input: текст SQL запроса
       Output: Dataframe"""
    host = 'mysql-master.yarustech.ru'
    port = '3306'
    database = 'itservice'
    username = 'admin'
    password = ''
    charset = 'utf8mb4'

    cnx = connect(
        host=host,
        port=port,
        charset=charset,
        database=database,
        user=username,
        password=password,
        auth_plugin='mysql_native_password')

    cursor = cnx.cursor(dictionary=True)
    query_set = "SET SESSION group_concat_max_len = 1000000;"

    cursor.execute(query_set)
    # cursor.execute(query, args)
    # print(query)
    result = pd.read_sql(query, cnx)
    # result = cursor.fetchall()
    cnx.close()
    return result

def endpoint_show(endpoint,server):
    """Input: тело запроса, сервер
       Output: ответ CURL запроса"""
    url = f"http://{server['host']}:{server['port']}{endpoint}"
    query_headers= {"X-API-KEY": server['api_key'],'X-DEVICE-ID':device_id}
    response = requests.get(url, headers=query_headers).json()
    #print(response['body'])
    return response['body']


def query_news(list_news):
    """Input: массив id новостей
       Output: текс SQL запроса с заголоками, датой и текстом статей"""

    query = """
                    SELECT news_post.id,  name, create_date, category_id,news_post_item_1.text
                    FROM news_post
                    RIGHT JOIN (
                            SELECT news_post_item.post_id,GROUP_CONCAT(text SEPARATOR "") as text 
                            FROM news_post_item
                            WHERE post_id in (%s)
                            GROUP BY post_id
                            ) as news_post_item_1 on news_post_item_1.post_id = news_post.id
                    WHERE text IS NOT NULL


                """
    in_p = ', '.join(list_news)
    query = query % in_p
    return query


def query_video(list_videos):
    """Input: массив id новостей
       Output: текс SQL запроса с заголоками, датой и текстом статей"""

    query = """
             SELECT id,user_id as user_id_load,video.name,description,insert_date,count_comment FROM itservice.video WHERE id in (%s);


             """
    in_p = ', '.join(list_videos)
    query = query % in_p
    return query


def compare_item(post_reply, columns, drop_columns, news=True):
    """Input: массив id новостей/видео со скорами.
       Перечень входных и выходных колонок определяются endpoint_showпараметрами columns и drop_columns
       Output: обьединенный датафрэйм со статьями"""
    df = pd.DataFrame(post_reply, columns=columns)
    # display(df)
    list_news = list(str(e) for e in df[columns[0]].to_list())
    query = query_news(list_news)

    if news == True:
        query = query_news(list_news)
    else:
        query = query_video(list_news)
    df_news = mysql(query)
    _df = pd.merge(df, df_news, how='left', left_on=[columns[0]], right_on=['id']).drop(columns=drop_columns)
    return _df


def trends_news(news):
    """Input: массив id новостей
       Output: датафрэйм со статьями"""
    list_news = list(str(e) for e in news)
    query = query_news(list_news)
    df_news = mysql(query)
    return df_news


def print_article(df_compare_item):
    """Выводит на экран тексты статей"""
    for date, row in df_compare_item.T.iteritems():
        print()
        print('id:', row[df_compare_item.columns[0]])
        print('Header:', row['name'])
        print('text_article:', row['text'], sep='\n')
        print()
