from CONSTANTS import TELEGA_TOKEN, POSTGRES_PWD, DISTRICTS, HELP_TEXT
import psycopg2
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import CommandHandler, ContextTypes, Application, \
    CallbackQueryHandler, ConversationHandler, MessageHandler, filters
from datetime import datetime

ZANOVO, HELP, ZAPROS, GUESTBOOK = "ЗАНОВО", "ПОМОЩЬ", ">>> обрабатываю запрос...", "/guestbook"
SQL_CHECK_UPDATING = "SELECT flag FROM update_flag_tbl"

global doctors_real_list, doctor_name, chat_id, poli_dict, user_mes_id


def sorry_we_r_updating():
    conn = psycopg2.connect(POSTGRES_PWD)
    cursor = conn.cursor()
    cursor.execute(SQL_CHECK_UPDATING)
    check_upd = cursor.fetchall()
    check_upd = check_upd[0][0]  # take first number only
    cursor.close()
    conn.close()
    return check_upd


######## get list of Polikliniks and available doctors from Postgres ########
def get_polikliniks_list_from_postgres():
    conn = psycopg2.connect(POSTGRES_PWD)
    cursor = conn.cursor()
    SQL = "SELECT poliklinik_name, poliklinik_id, doctor_name, doctor_id, doctor_real_name, doctor_real_id  " \
          "FROM polikliniki_tbl WHERE district = %s"
    cursor.execute(SQL, (users_district,))
    poli_data = cursor.fetchall()
    cursor.close()
    conn.close()

    # replace too long names of polikliniks
    for i in range(len(poli_data)):
        poli_data[i] = list(poli_data[i])
        poli_data[i][0] = poli_data[i][0].replace('"', '').replace("'", "'")

        if len(poli_data[i][0]) > 42:
            poli_data[i][0] = poli_data[i][0] \
                .replace("СПб ГБУЗ ", "") \
                .replace("Детская городская поликлиника", "ДГП") \
                .replace("Городская поликлиника", "ГП")

    # {[poliklinik_name, poliklinik_id] : [{doctor_name, doctor_id : doctor_real_name, doctor_real_id}]}

    global poli_dict
    poli_dict = dict()
    try:
        poli_dict[(poli_data[0][0], poli_data[0][1])] = dict()
        poli_dict[(poli_data[0][0], poli_data[0][1])][poli_data[0][2], poli_data[0][3]] = list()
        poli_dict[(poli_data[0][0], poli_data[0][1])][poli_data[0][2], poli_data[0][3]].append(
            [poli_data[0][4], poli_data[0][5]])
        for i in range(1, len(poli_data) - 1):
            if (poli_data[i][0], poli_data[i][1]) in poli_dict:
                if (poli_data[i][2], poli_data[i][3]) in poli_dict[(poli_data[i][0], poli_data[i][1])]:
                    poli_dict[(poli_data[i][0], poli_data[i][1])][poli_data[i][2], poli_data[i][3]].append(
                        [poli_data[i][4], poli_data[i][5]])
                else:
                    poli_dict[(poli_data[i][0], poli_data[i][1])][poli_data[i][2], poli_data[i][3]] = list()
                    poli_dict[(poli_data[i][0], poli_data[i][1])][poli_data[i][2], poli_data[i][3]].append(
                        [poli_data[i][4], poli_data[i][5]])
            else:
                poli_dict[(poli_data[i][0], poli_data[i][1])] = dict()
                poli_dict[(poli_data[i][0], poli_data[i][1])][poli_data[i][2], poli_data[i][3]] = list()
                poli_dict[(poli_data[i][0], poli_data[i][1])][poli_data[i][2], poli_data[i][3]].append(
                    [poli_data[i][4], poli_data[i][5]])

    except ValueError:
        return "error"
    # {('"Детская городская поликлиника №11" Детское поликлиническое отделение №23', 146):
    # {
    #    'Педиатр': ['Антропова Людмила Викторовна', '12'], ['Бокарева Юлия Викторовна', '34'],
    #    'Травматолог-ортопед': ['Филиппова Ирина Анатольевна', 'odijjkregver']
    #    }
    # turn to buttons format of python-telegram-bot
    keyboard_polikliniks_list = list()
    for key in poli_dict:
        keyboard_polikliniks_list.append([InlineKeyboardButton(str(key[0]), callback_data=str(key[1]))])

    return keyboard_polikliniks_list


##############
# update user_tbl after user request in Telegram
def postgres_user_tbl_new(chat_id, poliklinik_request, doctor_request, poliklinik_id, doc_real_name, doc_real_id,
                          doctor_id, users_district):
    conn = psycopg2.connect(POSTGRES_PWD)
    cursor = conn.cursor()
    sql = "INSERT INTO users_tbl(chat_id, poliklinik_request, doctor_request, poliklinik_id,\
    request_date, doc_real_name, doc_real_id, doctor_id, district_usr) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"

    current_date = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")

    cursor.execute(sql, (chat_id,
                         poliklinik_request,
                         doctor_request,
                         poliklinik_id,
                         current_date,
                         doc_real_name,
                         doc_real_id,
                         doctor_id,
                         users_district))
    conn.commit()

    cursor.close()
    conn.close()


# Stages
EXACT_DISTRICT, DOCTORS, MESS_FOR_USER, END_ROUTES, DOCTORS_REAL, START, ALL_TEXT_HENDLER, = range(7)


# main Telegram's interaction code
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send message on `/start`."""
    user = update.message.from_user

    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text=ZAPROS,
                                   # always same with button text
                                   reply_markup=ReplyKeyboardMarkup(
                                       [[KeyboardButton(ZANOVO)], [KeyboardButton(HELP)]],
                                       resize_keyboard=True))

    global chat_id
    chat_id = user['id']

    global user_mes_id
    user_mes_id = update.message.message_id  # get message ID to delete it in future to save space on screen

    # check that tbl is not in updating on current moment
    if sorry_we_r_updating() == 0:
        keyboard = list()
        for dist in DISTRICTS:
            keyboard.append([InlineKeyboardButton(str(dist), callback_data=str(dist))])

        # Send message with text and appended InlineKeyboard
        await context.bot.send_message(chat_id=update.effective_chat.id,
                                       text="1.Выберите ваш район:", reply_markup=InlineKeyboardMarkup(keyboard),
                                       )
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id,
                                       text="Извините, на данный момент мы обновляем информацию по поликлиникам. "
                                            "Попробуйте еще раз через несколько минут.")
        keyboard = 0  # to raise error

    return EXACT_DISTRICT


async def districts_f(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    global users_district
    users_district = str(query.data)

    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text=ZAPROS,
                                   # always same with button text
                                   reply_markup=ReplyKeyboardMarkup(
                                       [[KeyboardButton(ZANOVO)], [KeyboardButton(HELP)]],
                                       resize_keyboard=True))

    # check that tbl is not in updating on current moment
    if sorry_we_r_updating() == 0:
        keyboard = get_polikliniks_list_from_postgres()
        if keyboard == "error":
            await context.bot.send_message(chat_id=update.effective_chat.id,
                                           text="Что-то пошло не так. Попробуйте ЗАНОВО или чуть позже.")

        reply_markup = InlineKeyboardMarkup(keyboard)
        # Send message with text and appended InlineKeyboard
        await context.bot.send_message(chat_id=update.effective_chat.id,
                                       text=f"2.Район {users_district}, выберите поликлинику:",
                                       reply_markup=reply_markup)
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id,
                                       text="Извините, на данный момент мы обновляем информацию по поликлиникам. "
                                            "Попробуйте еще раз через несколько минут.")
        keyboard = 0  # to raise error

    try:
        for i in range(0, 3):
            await context.bot.deleteMessage(chat_id=update.effective_chat.id,
                                            message_id=user_mes_id + i)
    except:
        pass

    return DOCTORS


async def doctors_f(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    query = update.callback_query
    await query.answer()
    global poli_num
    poli_num = int(query.data)  # return sequence number of poliklinika

    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text=ZAPROS,
                                   # always same with button text
                                   reply_markup=ReplyKeyboardMarkup(
                                       [[KeyboardButton(ZANOVO)], [KeyboardButton(HELP)]],
                                       resize_keyboard=True))

    global doctors_dict
    keyboard_doctors_list = list()
    for key in poli_dict:
        if poli_num in key:
            doctors_dict = poli_dict[key]
            global poliklinika
            poliklinika = key[0]
            global poliklinik_id
            poliklinik_id = key[1]
            break

    for doctors in doctors_dict:
        keyboard_doctors_list.append([InlineKeyboardButton(str(doctors[0]), callback_data=str(doctors[1]))])

    keyboard = keyboard_doctors_list

    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text="3.Выберите профессию врача из доступных:", reply_markup=reply_markup
                                   )

    try:
        for i in range(3, 6):
            await context.bot.deleteMessage(chat_id=update.effective_chat.id,
                                            message_id=user_mes_id + i)
    except:
        pass

    return DOCTORS_REAL


async def doctors_REAL_f(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    query = update.callback_query
    await query.answer()
    global doctor_id
    doctor_id = str(query.data)  # return profession of doc

    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text=ZAPROS,
                                   # always same with button text
                                   reply_markup=ReplyKeyboardMarkup(
                                       [[KeyboardButton(ZANOVO)], [KeyboardButton(HELP)]],
                                       resize_keyboard=True))

    keyboard_doctors_real_list = list()
    global doctors_real_list
    global doctor_name
    doctor_name = ""
    doctors_real_list = list()
    for key in doctors_dict:
        if doctor_id in key:
            doctors_real_list = doctors_dict[key]  # return doctor_real_name & doctor_real_id
            doctor_name = key[0]
            break

    for doctors in doctors_real_list:
        keyboard_doctors_real_list.append([InlineKeyboardButton(str(doctors[0]), callback_data=str(doctors[1]))])

    reply_markup = InlineKeyboardMarkup(keyboard_doctors_real_list)
    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text="4.Выберите имя врача из доступных:", reply_markup=reply_markup)

    try:
        for i in range(6, 8):
            await context.bot.deleteMessage(chat_id=update.effective_chat.id,
                                            message_id=user_mes_id + i)
    except:
        pass

    return MESS_FOR_USER


async def mess_for_user_f(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    doctor_real_id = str(query.data)
    await query.answer()

    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text="Чтобы сделать новый или еще один запрос отправьте любой " \
                                        "текст или нажмите кнопку ЗАНОВО",
                                   # always same with button text
                                   reply_markup=ReplyKeyboardMarkup(
                                       [[KeyboardButton(ZANOVO)], [KeyboardButton(HELP)]],
                                       resize_keyboard=True))
    doctor_real_name = "UNKNOWN"
    for anyitems in doctors_real_list:
        if doctor_real_id in anyitems:
            doctor_real_name = anyitems[0]
            break

    try:
        await query.edit_message_text(text="##################\n"
                                           "Уведомления на номерки поставлены по следующим параметрам:\n"
                                           f"{poliklinika}\n"
                                           f"{doctor_name}\n"
                                           f"{doctor_real_name}\n\n"
                                           f"Попробуем отследить появление талонов и сообщить вам!\n"
                                           "##################\n")
    except:
        await context.bot.send_message(chat_id=update.effective_chat.id,
                                       text="Произошла ошибка :( \n"
                                            "Пожалуйста, выбирайте данные из последнего предложенного списка кнопок "
                                            "или начинайте заново, если желаете что-то изменить."
                                            "Иначе я путаюсь и не могу отработать правильно ваш запрос.")

    postgres_user_tbl_new(chat_id, poliklinika, doctor_name, poliklinik_id, doctor_real_name, doctor_real_id, doctor_id,
                          users_district)
    return ConversationHandler.END


async def help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(text=HELP_TEXT
                                    )
    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text=f"Чтобы установить заявки или оформить заявку сначала - нажите кнопку {ZANOVO} "
                                        "или отправьте любой текст в "
                                        f"сообщении. Чтобы открыть эту справку нажмите кнопку {HELP}.",
                                   # always same with button text
                                   reply_markup=ReplyKeyboardMarkup([[KeyboardButton(ZANOVO)], [KeyboardButton(HELP)]],
                                                                    resize_keyboard=True))


async def guestbook(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:    # guestbook for users
    user_message = update.message.text
    if user_message:
        user_message = user_message[4:]
        user_id = update.message.chat_id
        current_date = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")

        try:
            conn = psycopg2.connect(POSTGRES_PWD)
            cursor = conn.cursor()
            SQL = "INSERT INTO users_guestbook_tbl(user_id, comment, com_date) VALUES (%s, %s, %s);"
            cursor.execute(SQL, (user_id, user_message, current_date))
            conn.commit()
            cursor.close()
            conn.close()

            await update.message.reply_text("Спасибо за отзыв!")
        except:
            await update.message.reply_text(
                "База данных сейчас обновляется.\nПопробуйте позже и извините за неудобства!")

    else:
        pass
    return ConversationHandler.END


# cancel for cancel conversation

def cancel(update, _):
    return ConversationHandler.END


def main() -> None:
    """Run the bot."""
    application = Application.builder().token(TELEGA_TOKEN).build()

    # Обработчик ConversationHandler() имеет три основные точки, которые необходимо определить для ведения беседы
    conv_handler = ConversationHandler(
        # 1.  Разговор можно запустить по команде, отправленной пользователем (в данном случае /start)
        entry_points=[CommandHandler("start", start),
                      MessageHandler(filters.Text(ZANOVO), start),
                      MessageHandler(filters.Text(HELP), help),
                      MessageHandler(filters.Regex("^555"), guestbook),
                      MessageHandler(filters.Regex(fr"^(?!.*{HELP}).*$"), start)
                      ],

        # 2. Представляет собой словарь, в котором ключ, это этап разговора,
        # который явно возвращает функция обратного вызова,
        # при этом высылает или отвечает на сообщение или передает кнопки для выбора и т.д
        states={
            EXACT_DISTRICT: [CallbackQueryHandler(districts_f)],
            DOCTORS: [CallbackQueryHandler(doctors_f)],
            DOCTORS_REAL: [CallbackQueryHandler(doctors_REAL_f)],
            MESS_FOR_USER: [CallbackQueryHandler(mess_for_user_f)],
        },

        # 3. точка выхода из разговора. Разговор заканчивается, если функция обработчик сообщения
        # явно возвращает return ConversationHandler.END
        fallbacks=[MessageHandler('cancel', cancel)],
        allow_reentry=True  # allow to start from the beginning of converstion!
    )

    # Add ConversationHandler to application that will be used for handling updates
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("start", start))
    application.run_polling()


if __name__ == "__main__":
    asyncio.run(main())
