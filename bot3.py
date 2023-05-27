import telebot
from telebot.types import InputMediaPhoto
from telebot import types
import json
import time

href = '/home/ya/Документы/Projects/vkproject/support/'
with open(href + 'config.json', 'r') as file: conf = json.load(file)

TOKEN, CHAT_ID = conf['account']['token_tg'] , conf['techconf']['chat_id']
bot = telebot.TeleBot(TOKEN)


#
def update_table() -> None:
    import psycopg2 as db
    import json
    with open(href + 'config.json', 'r') as file: conf = json.load(file)
    co_data = conf['connect'] 

    #connect with server
    conn_string = f'dbname={co_data["base"]} host={co_data["host"]} \
    user={co_data["user"]} password={co_data["password"]}'

    conn = db.connect(conn_string)
    cur = conn.cursor()

    query = 'UPDATE motorsport SET is_watch = 1 WHERE is_watch IS NULL'

    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


#
def get_data() -> dict:
    with open(href + 'inter.json', 'r') as file2: data = json.load(file2)

    data_dict = {}
    for i, elem in enumerate(data['data']):
        data_dict[i] = {'text': elem[0], 'pages':elem[1], 'tag': elem[2]}
        

    return data_dict


#
def formatting(data_dict: dict) -> tuple or list:
    
    row_list = []
    for key in data_dict.keys():
        
        text = data_dict[key]['tag'].upper() + '\n' + data_dict[key]['text']
        text = text[:1023]

        if len(data_dict[key]['pages']) == 0:

            data_dict[key]['pages'] = [conf['pass_links'][data_dict[key]['tag']]]
            row_list.append((data_dict[key]['pages'][0], text))
        
        else:
            media = []
            pages = data_dict[key]['pages']
        
            for i, page in enumerate(pages):
                if i == 0:
                    media.append(InputMediaPhoto(page, caption=text)) 
                else:
                    media.append(InputMediaPhoto(page))
        
            row_list.append(media)
    
    return row_list


#send content
def send_content():

    try:
        
        args = formatting(get_data())

    except Exception:
        bot.send_message(chat_id=CHAT_ID,
                               text='<b>News not exists</b>',
                               parse_mode='HTML')
        return None

    for arg in args:
        try:
            print('trying')
            if type(arg) != tuple:
                bot.send_media_group(chat_id=CHAT_ID, media=arg)
                time.sleep(3)
            else:
                bot.send_photo(chat_id=CHAT_ID,
                                    photo=arg[0],
                                    caption=arg[1])
        #предпологается ошибка RetryAfter                            )
        except Exception as e:
            print(e)
            print('error')
            n_sec = int(str(e).split(' ')[-2])
            time.sleep(n_sec + 3)
            if type(arg) == types.MediaGroup :
                bot.send_media_group(chat_id=CHAT_ID, media=arg)
                time.sleep(3)
            else:
                bot.send_photo(chat_id=CHAT_ID,
                                    photo=arg[0],
                                    caption=arg[1])

    #ставим единицу в postgres тем постам, которые были отправлены в группу
    update_table()
    
    #чистим промежуточный файл
    with open(href + 'inter.json', 'w') as file:
        file.write('')

if __name__ == "__main__":
    send_content()
