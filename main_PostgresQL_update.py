# libs for Postgres
# !!!!! do not forget to install Postgres Command Line Tools if you use Windows and !!!!!
# !!!!! add the path to the bin folder in the PATH variable to use psycopg v3 !!!!!
import psycopg

from CONSTANTS import HEADERS, POSTGRES_PWD, POLIKLINIKI, TELEGA_TOKEN, MYID
from datetime import datetime

import asyncio
import aiohttp

import telegram

from sys import platform    # check OS cause in case of WIN it needs to add special event loop in async for psycopg

# start Postgres update ######################################
conn = psycopg.connect(POSTGRES_PWD)
cursor = conn.cursor()

# change update indicator to 1 to show that DB is under update
current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
sql_flag_update = "UPDATE update_flag_tbl SET (flag, last_update) = (%s, %s)"
cursor.execute(sql_flag_update, (1, current_date))
conn.commit()

sql_polikliniki_tbl = "INSERT INTO polikliniki_tbl(district, poliklinik_id, poliklinik_name,\
doctor_name, tickets, nearest_date, doctor_id, doctor_real_name, doctor_real_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"

# clear table before update
cursor.execute("DELETE FROM polikliniki_tbl")
conn.commit()


# start main cycle ###########################################
# 2_2
async def async_aiohttp_session(sess, url_json, poliklinik_id, district, poliklinik_name):
    async with sess.get(url=url_json) as response:
        print("sync_aiohttp_session")
        print(f"Поликлиника:{poliklinik_id}")
        response_json = await response.json(content_type=None)
        return district, poliklinik_id, poliklinik_name, response_json, sess


# 2_1
async def get_data_from_gorzdrav(district):
    print(district)
    async with aiohttp.ClientSession(headers=HEADERS, connector=aiohttp.TCPConnector()) as sess:
        get_html_task = list()

        for poliklinik_name in POLIKLINIKI[district][1]:
            poliklinik_id = int(POLIKLINIKI[district][1][poliklinik_name])
            # print(f"Поликлиника: {poliklinik_id}")
            # poliklinik_id_list.append(poliklinik_id)
            # poliklinik_name_list.append(key)
            url_json = f'https://gorzdrav.spb.ru/_api/api/v2/schedule/lpu/{poliklinik_id}/specialties'
            get_html_task.append(asyncio.ensure_future(async_aiohttp_session(sess,
                                                                             url_json,
                                                                             poliklinik_id,
                                                                             district,
                                                                             poliklinik_name)))

        got_data_session_1 = await asyncio.gather(*get_html_task, return_exceptions=True)
        return got_data_session_1


# 3_2
async def async_aiohttp_session_2(sess2, url_json_doctors, doctor_id, poliklinik_id, district, doctor_name,
                                  poliklinik_name):
    print("async_aiohttp_session_2 ###########################################")

    async with sess2.get(url=url_json_doctors) as response:
        print(f"session_2 - doctor:{doctor_id}")

        response_json = await response.json(content_type=None)
        # print(response)
        print(response_json)
        # sess2 MUST be returned otherwise session will be closed!
        # don't put AWAIT to return, it causes to ssl error
        return district, poliklinik_id, poliklinik_name, doctor_id, doctor_name, response_json, sess2


# 3_1
async def get_data_from_gorzdrav_doctors(got_data_session_1):
    async with aiohttp.ClientSession(headers=HEADERS, connector=aiohttp.TCPConnector()) as sess2:

        get_doctors_task = list()

        for i in range(len(got_data_session_1)):
            poli_resp_dictlist = got_data_session_1[i][3]['result']
            district = got_data_session_1[i][0]
            poliklinik_id = got_data_session_1[i][1]
            poliklinik_name = got_data_session_1[i][2]

            for s in range(len(poli_resp_dictlist)):
                doctor_spec_id = poli_resp_dictlist[s]["id"]

                # otherwise get error during request
                doctor_spec_id = doctor_spec_id.replace("+", "%2B").replace("/", "%2F").replace("=", "%3D")
                doctor_spec_name = poli_resp_dictlist[s]["name"]

                # get reals doctors name
                url_json_doctors = f'https://gorzdrav.spb.ru/_api/api/v2/schedule/lpu/{poliklinik_id}/speciality/{doctor_spec_id}/doctors'
                print(f"{i} from {len(poli_resp_dictlist)}")
                print(f"Поликлиника:{poliklinik_id}")
                print(f"Доктор:{doctor_spec_id}")

                get_doctors_task.append(
                    asyncio.ensure_future(async_aiohttp_session_2(sess2,
                                                                  url_json_doctors,
                                                                  doctor_spec_id,
                                                                  poliklinik_id,
                                                                  district,
                                                                  doctor_spec_name,
                                                                  poliklinik_name)))

        got_full_data = await asyncio.gather(*get_doctors_task, return_exceptions=True)
        return got_full_data


# 4
async def postgres_update(got_full_data):
    async with await psycopg.AsyncConnection.connect(POSTGRES_PWD) as aconn:
        async with aconn.cursor() as cur:
            for i in range(len(got_full_data)):

                district = got_full_data[i][0]
                poliklinik_id = got_full_data[i][1]
                poliklinik_name = got_full_data[i][2]
                doctor_spec_id = got_full_data[i][3]
                doctor_spec_name = got_full_data[i][4]
                try:
                    real_doctors_json = got_full_data[i][5]["result"]
                    for k in range(len(real_doctors_json)):
                        doctor_real_name = real_doctors_json[k]["name"]
                        tickets = real_doctors_json[k]["freeTicketCount"]
                        nearest_date = real_doctors_json[k]["nearestDate"]
                        doctor_real_id = real_doctors_json[k]["id"]

                        await cur.execute(sql_polikliniki_tbl,
                                          (district, poliklinik_id, poliklinik_name,
                                           doctor_spec_name, tickets, nearest_date, doctor_spec_id,
                                           doctor_real_name, doctor_real_id))
                        await aconn.commit()
                # in case some wrong data from server(success false instead of result)
                except KeyError:
                    print(got_full_data[i][5])
                    pass


# 1
async def main_async_fun():
    for district in POLIKLINIKI:
        got_data_session_1 = await get_data_from_gorzdrav(district)
        indices_list = list()  # list of indices to remove (with error response from server)

        for i in range(len(got_data_session_1)):
            if 'result' not in got_data_session_1[i][3]:  # 'result' is only one correct response
                indices_list.append(i)

        if indices_list:  # delete all incorrect responses
            got_data_session_1 = list(got_data_session_1)
            got_data_session_1 = [i for j, i in enumerate(got_data_session_1) if j not in indices_list]

        got_full_data = await get_data_from_gorzdrav_doctors(got_data_session_1)

        await postgres_update(got_full_data)


# FOR WINDOWS  only: On Windows Psycopg is not compatible with the default ProactorEventLoop.
# Require to use a different loop, for instance the SelectorEventLoop.
if platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

start = datetime.now()
asyncio.run(main_async_fun())
print(str(datetime.now() - start))

# change update indicator to 0 to show that DB update is finished
current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
sql_flag_update = "UPDATE update_flag_tbl SET (flag, last_update) = (%s, %s)"
cursor.execute(sql_flag_update, (0, current_date))
conn.commit()

# check qty of records in DB after update
sql_check_records = "SELECT COUNT (district) FROM polikliniki_tbl"
cursor.execute(sql_check_records)
records_qty = cursor.fetchall()
print(f"Records in DB: {records_qty[0][0]}")
cursor.close()
conn.close()


# inform to Telega
async def inform_to_telega():
    bot = telegram.Bot(TELEGA_TOKEN)
    async with bot:
        await bot.send_message(text=f'Records in DB: {records_qty[0][0]}\n'
                                    f'Elapsed time: {str(datetime.now() - start)}', chat_id=MYID)


asyncio.run(inform_to_telega())
