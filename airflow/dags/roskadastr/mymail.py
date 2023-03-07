import datetime
import os
import exchangelib
from exchangelib import Credentials, Account, Configuration
import pendulum
from datetime import timedelta

dt = pendulum.from_format('2023-03-03', 'YYYY-MM-DD')

def connect(server, email, username, password):
    credentials = Credentials(username=username, password=password)
    config = Configuration(service_endpoint=server, credentials=credentials)
    return Account(primary_smtp_address=email, autodiscover=True, config=config, access_type=exchangelib.DELEGATE)

server = 'https://mail.dagbti.ru/EWS'
email = 'rosreestrin@dagbti.ru'
username = 'DAGBTI\\rosreestrin'
password = 'SVngeM2'

account = connect(server, email, username, password)
#credentials = exchangelib.Credentials(username='DAGBTI\\rosreestrin', password='SVngeM2')
#account = exchangelib.Account(
#    primary_smtp_address='itcontrol@dagbti.ru', credentials=credentials,
#    autodiscover=True, access_type=exchangelib.DELEGATE
#)


try:
    path_str = account.root / 'Корневой уровень хранилища' / 'Входящие'
except exchangelib.errors.ErrorFolderNotFound:
    print('Неверно задано имя папки!')

#for item in account.inbox.all().order_by('-datetime_received')[:100]:
# for item in path_str.all().values_list('datetime_received', 'message_id'):
#     print(item.subject, item.sender, item.datetime_received)

emails_id = []
for item in path_str.all().order_by('-datetime_received')[:1]:
    datetime_received = item.datetime_received + timedelta(hours=3)
    if dt.date() < datetime_received.date():
        continue
    elif dt.date() > datetime_received.date():
        break
    emails_id.append(item.message_id)

for e in emails_id:
    item = path_str.get(message_id=e)
    if item:
        date = dt.date()
        for attach in item.attachments:
            if isinstance(attach, exchangelib.FileAttachment):
                local_path = os.path.join(pathz, attach.name)
                with open(local_path, 'wb') as f:
                    f.write(attach.content)


        if re.search('.*КОДИРОВКА.*', subject):
            date = item.datetime_received + timedelta(hours=3)
            pathz += date.strftime("%m.%d.%Y %H_%M_%S") + '/'
            if not pathlib.Path(pathz).exists():
                pathlib.Path(pathz).mkdir(parents=True, exist_ok=True)
                pathz += sender + '/'
                pathlib.Path(pathz).mkdir(parents=True, exist_ok=True)

    for item in path_str.all()[:1]:
        print(item.subject, item.sender, item.datetime_received)
#         pathz = './old/control/'
#         date = item[0] + timedelta(hours=3)
#         pathz += date.strftime("%m.%d.%Y %H_%M_%S") + '/'
#         if not pathlib.Path(pathz).exists():
#             emails_id.append(item[1])


# parser = argparse.ArgumentParser(description='Импорт xls')
# parser.add_argument(
#     '--folder',
#     type=pathlib.Path,
#     required=True,
#     help='Путь до файлов')
# args = parser.parse_args()

# def connect(server, email, username, password):
#     creds = Credentials(username=username, password=password)
#     config = Configuration(service_endpoint="https://mail.dagbti.ru/EWS", credentials=creds)
#     return Account(primary_smtp_address=email, autodiscover=True, config=config, access_type=DELEGATE)

# server = 'mail.dagbti.ru/ews'
# email = 'itcontrol@dagbti.ru'
# username = 'itcontrol'
# password = 'GbuDtk2021'

# account = connect(server, email, username, password)

#
#
# try:
#     emails_id = []
#     for item in path_str.all().values_list('datetime_received', 'message_id'):
#         pathz = './old/control/'
#         date = item[0] + timedelta(hours=3)
#         pathz += date.strftime("%m.%d.%Y %H_%M_%S") + '/'
#         if not pathlib.Path(pathz).exists():
#             emails_id.append(item[1])
#
#     for e in emails_id:
#         item = path_str.get(message_id=e)
#         if item:
#             print(item.author.name)
#             sender = item.author.name
#             subject = item.subject
#             subject = subject.upper()
#             pathz = './old/control/'
#             if re.search('.*КОДИРОВКА.*', subject):
#                 date = item.datetime_received + timedelta(hours=3)
#                 pathz += date.strftime("%m.%d.%Y %H_%M_%S") + '/'
#                 if not pathlib.Path(pathz).exists():
#                     pathlib.Path(pathz).mkdir(parents=True, exist_ok=True)
#                     pathz += sender + '/'
#                     pathlib.Path(pathz).mkdir(parents=True, exist_ok=True)
#                     for attach in item.attachments:
#                         if isinstance(attach, exchangelib.FileAttachment):
#                             local_path = os.path.join(pathz, attach.name)
#                             with open(local_path, 'wb') as f:
#                                 f.write(attach.content)
#
#
# except NameError:
#     print('Неверно задано имя папки!')
#
# # cad_num_names = [
# #     'КАДАСТРОВЫЙ НОМЕР',
# #     'КАДАСТРОВЫЙ НОМЕР [ПОМЕЩЕНИЕ]',
# #     'КАДАСТРОВЫЙ НОМЕР [ОБЪЕКТ НЕЗАВЕРШЕННОГО СТРОИТЕЛЬСТВА]',
# #     'КАДАСТРОВЫЙ НОМЕР [ЗДАНИЕ]',
# #     'КАДАСТРОВЫЙ НОМЕР [ЗЕМЕЛЬНЫЙ УЧАСТОК]',
# #     'КАДАСТРОВЫЙ НОМЕР [СООРУЖЕНИЕ]',
# #     'КАД №',
# #     'КАД. №',
# #     'КАДАСТРОВЫЙ № ОКС',
# #     'КАДАСТРОВЫЙ НОМЕР ОКС',
# # ]
# # code_group_names = [
# #     'КОД ГРУППЫ',
# #     'КОД ГРУПЫ'
# # ]
# #
# # pfile = 'files_list.pkl'
# # if pathlib.Path(pfile).exists():
# #     files_list = pickle.load(open(pfile, 'rb'))
# # else:
# #     files_list = []
# #
# #
# # for fxls in args.folder.glob('**/*.xlsx'):
# #
# #     fxls_absolute = str(fxls.absolute())
# #     if fxls.is_file() and '~' not in str(fxls.name) and fxls_absolute not in files_list:
# #
# #
# #         try:
# #             r = re.search('.*control/(.*?)/.*', fxls_absolute)
# #             date = datetime.datetime.strptime(r.group(1), "%m.%d.%Y %H_%M_%S").date()
# #
# #             r = re.search('.*control/.*/(.*)/.*?', fxls_absolute)
# #             sender = r.group(1)
# #
# #             try:
# #                 r = re.search('.*control/.*/.*/.*(\d\d\.\d\d\.\d\d\d\d?).*\(', fxls_absolute)
# #                 date_in = datetime.datetime.strptime(r.group(1), "%d.%m.%Y").date()
# #             except:
# #                 date_in = None
# #
# #             try:
# #                 r = re.search('.*control/.*/.*/.*?\((.*?)\).*', fxls_absolute)
# #                 name_in = r.group(1)
# #             except:
# #                 name_in = ''
# #
# #
# #             df = pd.read_excel(fxls.absolute(), header=None)
# #             i = 0
# #             _find = True
# #             for c in df.iloc[:, 2]:
# #                 if c is not numpy.nan:
# #                     _find = False
# #                     break
# #                 i += 1
# #             if _find:
# #                 print("- Not Find ROW Start Table")
# #
# #             cols = []
# #             j = 0
# #             for c in df.iloc[i]:
# #                 c = str(c)
# #                 if c == 'nan':
# #                     c = 'NaN' + str(j)
# #                     j += 1
# #                 if c.upper() not in cols:
# #                     cols.append(c.upper())
# #                 else:
# #                     cols.append(c.upper() + str(j))
# #                     j += 1
# #             df = df.drop(list(range(i+1)))
# #             df.columns = cols
# #
# #             cad_num_name = None
# #             code_group_name = None
# #             for c in cols:
# #                 if not cad_num_name and c in cad_num_names:
# #                     cad_num_name = c
# #                 if not code_group_name and c in code_group_names:
# #                     code_group_name = c
# #
# #             if code_group_name == None or cad_num_name == None:
# #                 print(f"Не найден заголовок {fxls_absolute}")
# #                 raise Exception("Не найден заголовок")
# #
# #             j = 0
# #             for index, row in df.iterrows():
# #
# #
# #                 if row[cad_num_name] is not numpy.nan:
# #
# #                     c = Control(
# #                         cad_num = row[cad_num_name],
# #                         code = row[code_group_name],
# #                         date = date,
# #                         date_in = date_in,
# #                         name_in = name_in,
# #                         sender = sender
# #                     )
# #                     cur_session.add(c)
# #                     cur_session.commit()
# #                     j += 1
# #             print(f"- Added rows - {j}")
# #             files_list.append(fxls_absolute)
# #             pickle.dump(files_list, open(pfile, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
# #
# #
# #         except Exception as e:
# #             print(f"ERROR LOAD ЗУ SHEET")
# #             print(fxls_absolute)
# #             print(e)
# #
# #
# # print("end")