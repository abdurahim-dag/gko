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
import xmltodict
import pandas as pd
import numpy


from dbmodel import Reestrin, Xmls, Reestrin_pdf
from dbengine import cur_session

def unzip(file):
    def unpack_zipfile(filename, extract_dir, encoding='cp437'):
        ##filename = filename.decode('utf-8')
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

def get_reestr_name(f):
    return re.search('.*?\d\d\.\d\d.\d\d\d\d(.*?)/.*', str(f.absolute())).group(1)

for folder in args.folder.iterdir():

    if folder.is_dir():

        # only for 2021
        iterate_folders(folder)

        try:
            n = folder.name
            date = re.search('.*?(\d\d\.\d\d.\d\d\d\d).*', n).group(1)
        except:
            try:
                n = str(folder.absolute())
                date = re.search('.*?(\d\d\d\d).*', n).group(1)
            except:
                print(f"ERROR DATE {n}")
                continue

        # ---> PDF
        def sopr_to_db(fpdf):
            name_in = get_reestr_name(fpdf)
            if len(date) == 4:
                datez = re.search('.*?(\d\d\.\d\d).*', str(fpdf.parent.absolute())).group(1) + '.' + date
            else:
                datez = date
            r_pdf = Reestrin_pdf(
                date=datetime.datetime.strptime(datez, "%d.%m.%Y").date(),
                name = name_in,
                file_name = fpdf.name,
                pdf = open(fpdf, 'rb').read(),
            )
            cur_session.add(r_pdf)
            cur_session.commit()

            print(f"SOPR - {fpdf.name}")

        for fpdf in folder.glob('**/*.pdf'):
            sopr_to_db(fpdf)
        for fpdf in folder.glob('**/*.png'):
            sopr_to_db(fpdf)

        # ---> XLS
        for fxls in folder.glob('**/*.xlsx'):

            if fxls.is_file() and '~' not in str(fxls.name):

                name_in = get_reestr_name(fxls)

                if len(date) == 4:
                    datez = re.search('.*?(\d\d\.\d\d).*', str(fxls.parent.absolute())).group(1) + '.' + date
                else:
                    datez = date

                try:
                    df_xls = pd.read_excel(fxls.absolute(), header=None, engine='openpyxl',)
                except:
                    df_xls = pd.read_excel(fxls.absolute(), header=None, engine='openpyxl',)
                i = 0
                options = []
                for c in df_xls.iloc[:, 1]:
                    if c is not numpy.nan:
                        break
                    options.append(df_xls.iloc[i, 0])
                    i += 1
                if i == 0:
                    print(f"0 ROW Start - {fxls.absolute()}")

                options = '|'.join(str(e) for e in options)

                r = Reestrin(
                    date=datetime.datetime.strptime(datez, "%d.%m.%Y").date(),
                    name = name_in,
                    description=str(fxls.parents[0]),
                    file_name = fxls.name,
                    options = options
                )

                cur_session.add(r)
                cur_session.commit()

                cols = []
                j = 0
                for c in df_xls.iloc[i]:
                    c = str(c)
                    if c == 'nan':
                        c = 'NaN' + str(j)
                        j += 1
                    if c.upper() not in cols:
                        cols.append(c.upper())
                    else:
                        cols.append(c.upper() + str(j))
                        j += 1
                df_xls = df_xls.drop(list(range(i+1)))
                df_xls.columns = cols

                result = df_xls.to_json(orient="records")
                parsed = json.loads(result)

                with open("json_data.json", "w",) as json_file:
                    json.dump(parsed, json_file, indent = 2, ensure_ascii=False)

                result = None
                parsed = None

                xmltext = open("json_data.json", 'rb').read()
                if xmltext[:3] == b'\xef\xbb\xbf':
                    s = xmltext.decode("utf-8-sig")
                else:
                    s = xmltext.decode("utf-8")
                s = re.sub(r"\t", "", s)
                s = re.sub(r"\r", "", s)
                s = re.sub(r"\n", "", s)
                xmltext = bytes(s, 'utf-8')

                f = open("json_data.json", 'wb')
                f.write(xmltext)
                f.close()


                f = open("json_data.json", 'r')
                dbpsycop.cursor.execute("INSERT INTO public.xlss \
                (reestrin_id, file_name, xlss) VALUES (%s,%s,%s)", (r.id, fxls.name, f.read(),))

                dbpsycop.conn.commit()

                print(fxls.name)
                df_xls = None
                f.close()

        # ---> XML
        for fxml in folder.glob('**/*.xml'):

            if fxml.is_file():

                name_in = get_reestr_name(fxml)

                if len(date) == 4:
                    datez = re.search('.*?(\d\d\.\d\d).*', str(fxml.parent.absolute())).group(1) + '.' + date
                else:
                    datez = date

                xmltext = open(fxml, 'rb').read()
                if xmltext[:3] == b'\xef\xbb\xbf':
                    s = xmltext.decode("utf-8-sig")
                else:
                    s = xmltext.decode("utf-8")

                xml_to_dict = xmltodict.parse(s)

                options = json.dumps(xml_to_dict['ListForRating']['ListInfo'])

                r = Reestrin(
                    date=datetime.datetime.strptime(datez, "%d.%m.%Y").date(),
                    description=str(fxml.parents[0]),
                    name = name_in,
                    file_name = fxml.name,
                    options = options
                )

                cur_session.add(r)
                cur_session.commit()

                with open("json_data.json", "w",) as json_file:
                    json.dump(xml_to_dict, json_file, indent = 2, ensure_ascii=False)

                f = open("json_data.json", 'rb')
                xmltext = f.read()
                f.close()

                if xmltext[:3] == b'\xef\xbb\xbf':
                    s = xmltext.decode("utf-8-sig")
                else:
                    s = xmltext.decode("utf-8")
                s = re.sub(r"\t", "", s)
                s = re.sub(r"\r", "", s)
                s = re.sub(r"\n", "", s)
                xmltext = bytes(s, 'utf-8')

                f = open("json_data.json", 'wb')
                f.write(xmltext)
                f.close()

                f = open("json_data.json", 'r')
                dbpsycop.cursor.execute("INSERT INTO public.xmls \
                (reestrin_id, file_name, xml) VALUES (%s,%s,%s)", (r.id, fxml.name, f.read(),))
                dbpsycop.conn.commit()
                f.close()

                print(fxml.name)



print('end')
