import re
import io
import os
import shutil
import zipfile
import argparse
import pathlib
import datetime
import dbpsycop
import xml.etree.ElementTree as ET
import xmltodict, json
import psycopg2
import numpy
import pandas as pd

from dbmodel import Message, Acts, Out_FD, Acts_Objects_OK, Acts_Objects_Bad
from dbengine import Session

def unzip(file):
    def unpack_zipfile(filename, extract_dir, encoding='cp437'):
        with zipfile.ZipFile(filename) as archive:
            for entry in archive.infolist():
                try:
                    name = entry.filename.encode('cp437').decode(encoding)  # reencode!!!
                except:
                    name = entry.filename

                # don't extract absolute paths or ones with .. in them
                if name.startswith('/') or '..' in name:
                    continue

                target = os.path.join(extract_dir, *name.split('/'))
                os.makedirs(os.path.dirname(target), exist_ok=True)
                if not entry.is_dir():  # file
                    with archive.open(entry) as source, open(target, 'wb') as dest:
                        shutil.copyfileobj(source, dest)

    fpath = os.path.join(
        file.parent.absolute(),
        file.name)

    tpath = os.path.join(
        file.parent.absolute(),
        file.stem)

    if os.path.exists(tpath):
        shutil.rmtree(tpath)

    unpack_zipfile(fpath, tpath, encoding='cp866')


def iterate_folders(folder):
    for fzip in folder.glob('*.zip'):
        unzip(fzip)
    for fld in folder.iterdir():
        if fld.is_dir():
            iterate_folders(fld)


parser = argparse.ArgumentParser(description='Импорт xmls')
parser.add_argument(
    '--folder',
    type=pathlib.Path,
    required=True,
    help='Путь до файлов')
args = parser.parse_args()
rosreestr_in = []
count_folders = 0

year = re.search('.*(\d\d\d\d).*', str(args.folder)).group(1)
for folder in args.folder.iterdir():

    if folder.is_dir():

        iterate_folders(folder)

        #Где есть pdf - значит исход перечень
        #for pdf in folder.glob('*.pdf'):
        for pdf in folder.glob('**/*.docx'):
            count_folders += 1

            if pdf.is_file() and '~' not in str(pdf.absolute()):

                message_name = folder.name
                date = re.search('.*(\d\d\.\d\d).*', message_name).group(1) + f".{year}"
                date_in = datetime.datetime.strptime(date, "%d.%m.%Y").date()

                message_pdf = pdf
                message_pdf_name = pdf.name

                if 'docx' in str(pdf.name):
                    date_out = datetime.datetime.fromtimestamp(message_pdf.stat().st_mtime)
                else:
                    try:
                        date = re.search('.*(\d\d\.\d\d).*', message_pdf_name).group(1) + f".{year}"
                        date_out = datetime.datetime.strptime(date, "%d.%m.%Y").date()
                    except:
                        date_out = datetime.datetime.fromtimestamp(message_pdf.stat().st_mtime)




                name_in = pdf.parent.name

                m = Message(
                    date_in=date_in,
                    name_in = name_in,
                    date_out=date_out,
                    pdf = open(message_pdf, 'rb').read(),
                    pdf_name = message_pdf.name,
                    name = message_name,
                    type = 0
                )
                session = Session()
                session.add(m)
                session.commit()

                # Где есть ods-акты там есть XMLS
                count_xml = 0
                for ods in folder.glob('**/*.ods'):
                    if ods.is_file() and '~' not in str(ods.absolute()):

                        #считываем  и находим строку начала таблицы
                        df_cost_ok = pd.read_excel(ods.absolute(), sheet_name='II', header=None)
                        i = 0
                        for c in df_cost_ok.iloc[:, 1]:
                            if c is not numpy.nan:
                                break
                            i += 1
                        if i == 0:
                            print("Not Find ROW Start Table")
                            break

                        cols = []
                        j = 0
                        for c in df_cost_ok.iloc[i]:
                            c = str(c)
                            if c == 'nan':
                                c = 'NaN' + str(j)
                                j += 1
                            if c.upper() not in cols:
                                cols.append(c.upper())
                            else:
                                cols.append(c.upper() + str(j))
                                j += 1

                        df_cost_ok = df_cost_ok.drop(list(range(i+1)))
                        df_cost_ok.columns = cols
                        count_cost_ok = int(df_cost_ok.count()[0])


                        df_cost_bad = pd.read_excel(ods.absolute(), sheet_name='III', header=None)
                        i = 0
                        for c in df_cost_bad.iloc[:, 1]:
                            if c is not numpy.nan:
                                break
                            i += 1
                        if i == 0:
                            print("Not Find ROW Start Table")
                            break

                        cols = []
                        j = 0
                        for c in df_cost_bad.iloc[i]:
                            c = str(c)
                            if c == 'nan':
                                c = 'NaN' + str(j)
                                j += 1
                            if c.upper() not in cols:
                                cols.append(c.upper())
                            else:
                                cols.append(c.upper() + str(j))
                                j += 1

                        df_cost_bad = df_cost_bad.drop(list(range(i+1)))
                        df_cost_bad.columns = cols
                        count_cost_bad = int(df_cost_bad.count()[0])

                        act = Acts(
                            count_ok=count_cost_ok,
                            count_bad=count_cost_bad,
                            ods = open(ods, 'rb').read(),
                            ods_name = ods.name,
                            message_id=m.id
                        )
                        session = Session()
                        session.add(act)
                        session.commit()

                        for index, row in df_cost_ok.iterrows():
                            aok = Acts_Objects_OK(
                                acts_id = act.id,
                                cad_num = row['КАДАСТРОВЫЙ НОМЕР']
                            )
                            session = Session()
                            session.add(aok)
                            session.commit()

                        for index, row in df_cost_bad.iterrows():
                            abad = Acts_Objects_Bad(
                                acts_id = act.id,
                                cad_num = row['КАДАСТРОВЫЙ НОМЕР']
                            )
                            session = Session()
                            session.add(abad)
                            session.commit()

                        # обработка xmls
                        #Если есть zip то значит есть и xml
                        if list(ods.parents[0].glob('*.zip')):
                            for fxml in ods.parents[0].glob('**/COST*.xml'):
                                if fxml.is_file():
                                    count_xml += 1

                                    xmltext = open(fxml, 'rb').read()
                                    if xmltext[:3] == b'\xef\xbb\xbf':
                                        s = xmltext.decode("utf-8-sig")
                                    else:
                                        s = xmltext.decode("utf-8")

                                    xml_to_dict = xmltodict.parse(s)

                                    with open("json_data.json", "w",) as json_file:
                                        json.dump(xml_to_dict, json_file, indent = 2, ensure_ascii=False)

                                    xmltext = open("json_data.json", 'r').read()
                                    xmltext = re.sub(r"\t", "", xmltext)
                                    xmltext = re.sub(r"\r", "", xmltext)
                                    xmltext = re.sub(r"\n", "", xmltext)
                                    xmltext = bytes(xmltext, 'utf-8')

                                    f = open("json_data.json", 'wb')
                                    f.write(xmltext)
                                    f.close()

                                    decription=str(fxml.absolute())
                                    f = open("json_data.json", 'r')
                                    dbpsycop.cursor.execute("INSERT INTO public.out_xmls \
                                    (acts_id, decription, xml) VALUES (%s,%s,%s)", (act.id, decription, f.read(),))
                                    dbpsycop.conn.commit()
                                    f.close()


                            for fdxml in ods.parents[0].glob('**/FD*.xml'):
                                if fdxml.is_file():
                                    xmltext = open(fdxml, 'rb').read()
                                    if xmltext[:3] == b'\xef\xbb\xbf':
                                        s = xmltext.decode("utf-8-sig")
                                    else:
                                        s = xmltext.decode("utf-8")

                                    tree = ET.fromstring(s)
                                    root = tree
                                    groups = {}
                                    for g in root.iterfind('.//Group_Real_Estate'):
                                        groups[g.find('.//ID_Group').text] = g.find('.//Name_Group').text


                                    reality = []
                                    for r in root.iterfind('.//Real_Estate'):
                                        reality.append(
                                            {
                                                r.find('.//CadastralNumber').text: groups[r.attrib['ID_Group']]
                                            }
                                        )


                                    for r in reality:
                                        [(k, v)] = list(r.items())
                                        fd = Out_FD(
                                            cad_num=k,
                                            group=v,
                                            decription=str(fdxml.absolute()),
                                            acts_id = act.id,
                                        )
                                        session = Session()
                                        session.add(fd)
                                        session.commit()


                print(f"[{count_xml}] xmls in {folder.name}")

print(f"Folders - [{count_folders}]")






# rosreestr_in.append({
#     "date": folder.name
# })
# iterate_folders(folder)
# for fpdf in folder.glob('**/*.pdf'):
#     with open(fpdf, 'rb') as f:
#         r = Reestrin(
#             date=datetime.datetime.strptime(folder.name, "%d.%m.%Y").date(),
#             pdf=f.read(),
#             description=str(fpdf.parents[0]),
#             file_name = fpdf.name
#         )
#         session = Session()
#         session.add(r)
#         session.commit()
#     for fxml in fpdf.parents[0].glob('**/*.xml'):


#         xmltext = open(fxml, 'rb').read()
#         if xmltext[:3] == b'\xef\xbb\xbf':
#             s = xmltext.decode("utf-8-sig")
#         else:
#             s = xmltext.decode("utf-8")


#         xml_to_dict = xmltodict.parse(s)

#         with open("json_data.json", "w",) as json_file:
#             json.dump(xml_to_dict, json_file, indent = 2, ensure_ascii=False)

#         # try:
#         #     f = open('1', 'wb')
#         #     j = json.dump(o,f)

#         # except Exception:
#         #     print(e)


#         #xmltext = open("json_data.json", 'r').read()


#         xmltext = open("json_data.json", 'rb').read()
#         if xmltext[:3] == b'\xef\xbb\xbf':
#             s = xmltext.decode("utf-8-sig")
#         else:
#             s = xmltext.decode("utf-8")
#         s = re.sub(r"\t", "", s)
#         s = re.sub(r"\r", "", s)
#         s = re.sub(r"\n", "", s)
#         xmltext = bytes(s, 'utf-8')

#         f = open("json_data.json", 'wb')
#         f.write(xmltext)
#         f.close()



#         file = open("json_data.json", 'r')
#         dbpsycop.cursor.execute("INSERT INTO public.xmls \
#         (reestrin_id, file_name, xml) VALUES (%s,%s,%s)", (r.id, fxml.name, file.read(),))
#         # file = open("json_data.json", 'rb')
#         # sql = "COPY xmls(xml) FROM stdin"
#         # dbpsycop.cursor.copy_expert(sql, file)
#         dbpsycop.conn.commit()


#         # x = Xmls(
#         #     reestrin_id = r.id,
#         #     xml = xmltext,
#         #     file_name = fxml.name
#         # )
#         # session = Session()
#         # session.add(x)
#         # session.commit()
#         print(fxml.name)



#         # s = re.sub(r"\t", "", s)
#         # s = re.sub(r"\r", "", s)
#         # s = re.sub(r"\n", "", s)




#         # xmltext = bytes(s, 'utf-8')

#         # f = open(fxml, 'wb')
#         # f.write(xmltext)
#         # f.close()

#         # tree = ET.parse(fxml)
#         # root = tree.getroot()

#         # x = Xmls(
#         #     reestrin_id = r.id,
#         #     xml = open("json_data.json", 'r').read(),
#         #     file_name = fxml.name
#         # )
#         # session = Session()
#         # session.add(x)
#         # session.commit()
#         #     print({pathlib.Path(fxml).stat().st_size})
#         # except:
#         #     print(f"{pathlib.Path(fxml).stat().st_size} - ERROR")

# #cwd = os.path.join(folder.absolute(), type_reality + '.xlsx')
# #file_name
# #reestr_in_date_id

