import aiohttp
import psycopg
from psycopg.rows import dict_row
from CONSTANTS import HEADERS, POSTGRES_PWD, POLIKLINIKI
import telegram
import asyncio
from CONSTANTS import TELEGA_TOKEN, MYID
from sys import platform

# start Postgres update
conn = psycopg.connect(POSTGRES_PWD, row_factory=dict_row)
cursor = conn.cursor()


# inform user in case some troubles with access to db
async def inform_user_with_db_troubles():
    bot = telegram.Bot(TELEGA_TOKEN)
    async with bot:
        await bot.send_message(text="Что-то пошло не так с чтением данных из базы",
                               chat_id=MYID)


# connect to Postgres to get all data from users_tbl
try:
    sql_users_data = "SELECT * FROM users_tbl WHERE done = 0"
    cur_dict = conn.cursor()
    cur_dict.execute(sql_users_data)
    joined_tbl = cur_dict.fetchall()
    cursor.close()
    conn.close()
except:  # PEP8, sorry for this :(
    asyncio.run(inform_user_with_db_troubles())
    print("Some sheet happens")
    exit()
else:
    if not joined_tbl:
        print("Request returns empty list")
        exit()

# start Telegram dor distribution updated info

sql_update_user_tbl = "UPDATE users_tbl SET done = 1 WHERE " \
                      "chat_id = %s and " \
                      "doc_real_id = %s and " \
                      "done = 0"


async def aiohttp_session(chat_id, poliklinik_id, poliklinik_name, doctor_spec, doctor_spec_id, doc_real_name,
                          doctor_id, district, district_id, url_json_doctors, sess_):
    async with sess_.get(url=url_json_doctors) as response:
        print(district)
        print(poliklinik_name)
        print(doctor_id)
        print(doc_real_name)
        if response.status == 200:
            doctors_json = await response.json(content_type=None)
            doctors_json = doctors_json['result']
            for k in range(len(doctors_json)):
                if doctor_id == doctors_json[k]['id']:
                    tickets = doctors_json[k]['freeTicketCount']
                    try:
                        nearest_date = doctors_json[k]['nearestDate'][:10]  # 2022-11-08
                    except ValueError:
                        nearest_date = 0

                    if tickets > 0:
                        async with telegram.Bot(TELEGA_TOKEN):
                            await telegram.Bot(TELEGA_TOKEN).send_message(
                                text=f'ПОЯВИЛИСЬ НОМЕРКИ!\n'
                                     f'{district} район\n'
                                     f'{poliklinik_name}\n'
                                     f'{doctor_spec}\n'
                                     f'{doc_real_name}\n'
                                     f'Доступное количество: {tickets}\n'
                                     f'Ближайшая дата: {nearest_date}\n\n'
                                     f'Пройдите по ссылке для записи:'
                                     f'"https://gorzdrav.spb.ru/service-free-schedule#%5B%7B%22'
                                     f'district%22:%22{district_id}%22%7D,%7B%22'
                                     f'lpu%22:%22{poliklinik_id}%22%7D,%7B%22'
                                     f'speciality%22:%22{doctor_spec_id}%22%7D,'
                                     f'%7B%22doctor%22:%22{doctor_id}%22%7D%5D"\n\n'
                                     f'Заявка отработана. Для установки новой заявки '
                                     f'нажмите ==> /start', chat_id=chat_id)

                    # clear users_tbl where tickets > 0 as one time inform is enough but keep rows with tickets == 0
                        async with await psycopg.AsyncConnection.connect(POSTGRES_PWD) as aconn:
                            async with aconn.cursor() as cur:
                                await cur.execute(sql_update_user_tbl, (chat_id, doctor_id))
                                await aconn.commit()

        return sess_


async def gorzdrav_response():
    async with aiohttp.ClientSession(headers=HEADERS, connector=aiohttp.TCPConnector()) as sess:
        get_doctors_list = list()
        for rows in joined_tbl:
            # joined_tbl acts as dict (because psycopg2 returns type RealDictCursor)
            chat_id = int(rows['chat_id'])  # 5252203179
            poliklinik_id = rows['poliklinik_id']
            poliklinik_name = rows[
                'poliklinik_request']  # Городская поликлиника №52" Отделение общей врачебной практики
            doctor_spec = rows['doctor_request']  # Педиатр
            doctor_spec_id = rows['doctor_id'].replace("+", "%2B").replace("/", "%2F").replace("=", "%3D")
            doc_real_name = rows['doc_real_name']  # Черная Инесса Владимировна
            doctor_id = rows['doc_real_id']
            district = rows['district_usr']  # Выборгский

            district_id = POLIKLINIKI[district][0]  # convert districts name to districts_id

            url_json_doctors = f'https://gorzdrav.spb.ru/_api/api/v2/schedule/lpu/{poliklinik_id}/speciality/{doctor_spec_id}/doctors'

            get_doctors_list.append(asyncio.ensure_future(aiohttp_session(chat_id, poliklinik_id, poliklinik_name,
                                                                          doctor_spec, doctor_spec_id, doc_real_name,
                                                                          doctor_id, district, district_id,
                                                                          url_json_doctors, sess)))

            got_doctors_full_list = await asyncio.gather(*get_doctors_list, return_exceptions=True)

        return got_doctors_full_list


async def main():
    await gorzdrav_response()


if platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
