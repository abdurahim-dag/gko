import logging
import pathlib

import pandas as pd
from lxml import etree as ET


target_folder = r'c:\XML out'
target_file = r'D:\finds out.xlsx'

df: pd.DataFrame = pd.DataFrame(columns=['cad_num'])
cad_nums = []
for target_xml in pathlib.Path(target_folder).glob('*.xml'):
    with open(target_xml,"r",encoding="utf8") as file_xml:
        tree = ET.parse(file_xml)

        root = tree.getroot()
        objects = root.findall('**/cad_number')

        if len(objects) > 0:
            for obj in objects:
                cad_num = obj.text
                cad_nums.append(('cad_num', cad_num))
logging.info('Create xlsx')
df = df.from_records(cad_nums)
df.to_excel(target_file)
logging.info('End')
