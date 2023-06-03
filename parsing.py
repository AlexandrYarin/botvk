#import
import requests
from datetime import datetime
import psycopg2 as db
import json
import gc

href = '/home/ya/Документы/Projects/vkproject/support/'

#load config
with open(href + 'config.json', 'r') as file: conf = json.load(file)

co_data = conf['connect'] 
account = conf['account']
techconf = conf['techconf']
pass_links = conf['pass_links']

TOKEN = account['token_vk']
NAMES_GROUP = techconf['name_group']

VAL_POST = techconf['value_posts']
OFFSET = techconf['offset']

#connect with server
conn_string = f'dbname={co_data["base"]} host={co_data["host"]} \
user={co_data["user"]} password={co_data["password"]}'

conn = db.connect(conn_string)
cur = conn.cursor()

def get_data(count:int, offset:int) -> object:
    """
    Return: Function return file.jscon with given params
    count: value of posts we need
    offset: just offset"""
    
    URL = 'https://api.vk.com/method/wall.get'
    
    responses = []

    for name in NAMES_GROUP:

        params = {
            'domain':name, 'filter': 'owner', 'count': count,
            'offset': offset, 'access_token': TOKEN, 'v':5.95
            }
    
        response = requests.get(URL, params=params)

        responses.append(response.json())

        print(f'Status: {response.status_code}')
    
    return responses

def check_exists_strings() -> list:
    """
    Return: list of text by we check exists the same string in db
    """
    query_check = 'select post, links from motorsport'
    cur.execute(query_check)

    uniq_text = cur.fetchall()
    
    conn.close()
    
    return uniq_text


#formatting
def splitting(answer:object, val_posts:int, relevant_list: list) -> dict:
    """
    Return: dict of content that we need
    answer: list of three file.json which we need splitting on some blocks(pages, text, etc.)
    val_posts: value of posts
    """
    content_dict = {'f1':[], 'wec':[], 'ferrari':[]}

    #iteration of posts
    for i in range(len(answer)):
        
        tag = ''
        
        if i == 0: tag = 'f1'
        elif i == 1: tag = 'wec'
        else: tag = 'ferrari'
        
        for num_post in range(val_posts):
        
            #pull out data that we need
            if 'copy_history' in answer[i]['response']['items'][num_post].keys():
                post = answer[i]['response']['items'][num_post]['copy_history'][0]
            else:
                post = answer[i]['response']['items'][num_post]

            #MAKE POST
            #----------------------------------------------------------
            #first content
            content = post['attachments']
            #then datatime
            ts = datetime.fromtimestamp(post['date'])
            
            #then pages
            pages = [
                    elem['photo']['sizes'][-1]['url'] \
                        for elem in content if elem['type'] == 'photo'
                        ]
            #check avalible video(we dont need the video yet)
            if len(pages) == 0:
                pages.append(pass_links[tag])

            #finally text
            text = post['text'] if len(post['text']) > 0 else ''
            
            #----------------------------------------------------------
            
            if (text, pages) in relevant_list:
                continue
            else:
                #create dict of content
                content_dict[tag].append({num_post:{'date': ts, 'text': text, 'pages': pages, 'tag': tag}})
        
    return content_dict


def main(val_posts=10, offset=0, relevant_list=[]):
    """
    Return: clean result
    val_posts: see above
    offset: see above
    """
    answer = get_data(val_posts, offset)
    result = splitting(answer, val_posts, relevant_list)

    return result

def create_and_insert(result):
    data = []

    conn = db.connect(conn_string)
    cur = conn.cursor()

    for group in result.keys():
        for elem in result[group]:
            for k in elem:
                data.append((elem[k]['date'], elem[k]['text'], elem[k]['pages'], elem[k]['tag']))

    data_for_db = tuple(data)


    query = 'insert into motorsport (date, post, links, tag) \
    values(%s,%s,%s, %s)'
    try:
        cur.executemany(query, data_for_db)
        conn.commit()
        conn.close()
    except Exception as e:
        print(e)
        conn.rollback()

def parsing():
    #list of text that alredy exist in db
    not_relevant_list = check_exists_strings()

    result = main(VAL_POST, OFFSET, not_relevant_list)
    print(f'new files: {len(result)}')

    create_and_insert(result)


    del not_relevant_list
    gc.collect()


