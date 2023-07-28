import logging
import pathlib

import pandas as pd
from lxml import etree as ET


target_folder = r'c:\XML'
target_file = r'D:\finds.xml'
list_cad_nums_file = r'D:\cad_nums.xlsx'

cad_nums = []
xlsx = pd.ExcelFile(list_cad_nums_file)
df = pd.read_excel(xlsx, header=None)
for _, row in df.iterrows():
    cad_nums.append(row[0])

finds = ET.Element("objects")

for target_xml in pathlib.Path(target_folder).glob('*.xml'):
    with open(target_xml,"r",encoding="utf8") as file_xml:
        tree = ET.parse(file_xml)

        root = tree.getroot()
        parcels = root.findall('Objects/*/*')

        if len(buildings) > 0:
            for parcel in parcels:
                cad_num = parcel.attrib['CadastralNumber']
                if cad_num in cad_nums:
                    finds.append(parcel)

                    logging.warning('CadastralNumber %s is find', cad_num)

doc = ET.ElementTree(finds)
doc.write(target_file, encoding='utf-8', xml_declaration=True)

logging.info('End')
