import MySQLdb
import MySQLdb.cursors
import pymysql
from dotenv import load_dotenv
import pandas as pd
import attrs
from attrs_strict import type_validator
import os
import datetime
import numpy as np
import argparse

load_dotenv()

MYSQL_DB = os.getenv("MYSQL_DB", "anthology")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

db = MySQLdb.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    port=MYSQL_PORT,
    cursorclass=MySQLdb.cursors.DictCursor
)

@attrs.frozen
class PersonAcceptance(object):
    id: int = attrs.field(validator=type_validator())
    name: str = attrs.field(validator=type_validator())
    surname: str = attrs.field(validator=type_validator())
    birth_nation:str = attrs.field(validator=type_validator())
    birth_date: datetime.datetime =  attrs.field(validator=type_validator())
    gender:str = attrs.field(validator=type_validator())
    from_date: datetime.datetime =  attrs.field(validator=type_validator())

    @gender.validator
    def fits_value(self, attribute, value):
        if value.upper() not in ("M", "F", "T"):
            raise ValueError("invalid value for gender")

    @classmethod
    def import_row(cls, row):
        return cls(
            id = row['id'],
            name = row['nome'],
            surname = row['cognome'],
            birth_nation = row['nazione'],
            birth_date = row['data_nascita'],
            gender = row['genere'],
            from_date = row['ingresso']
        )
        
def main(input, output):
    persons = []
    df = pd.read_excel(input)
    df = df.replace([np.nan], [None])
    as_dict = df.to_dict('records')

    for row in as_dict:
        persons.append(PersonAcceptance.import_row(row))

    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    cursor = db.cursor()

    write_header = True

    dataframe = {
        "id": [],
        "ant_uuid": [],
        "nome": [],
        "cognome": [],
        "nazione": [],
        "data_nascita": [],
        "genere": [],
        "centro_1": [],
        "ingresso_1": [],
        "uscita_1": [],
        "motivazione_1": [],
        "centro_2": [],
        "ingresso_2": [],
        "uscita_2": [],
        "motivazione_2": [],
        "centro_3": [],
        "ingresso_3": [],
        "uscita_3": [],
        "motivazione_3": [],
        "centro_4": [],
        "ingresso_4": [],
        "uscita_4": [],
        "motivazione_4": [],
        "centro_5": [],
        "ingresso_5": [],
        "uscita_5": [],
        "motivazione_5": [],
    }

    for p in persons:

        dataframe['id'].append(p.id)
        dataframe['nome'].append(p.name)
        dataframe['cognome'].append(p.surname)
        dataframe['nazione'].append(p.birth_nation)
        dataframe['data_nascita'].append(p.birth_date)
        dataframe['genere'].append(p.gender)

        sql = """
            SELECT
                HEX(P.uuid) AS ant_uuid,
                %s AS id_sprar,
                P.name AS nome,
                P.surname AS cognome,
                S.name AS centro,
                AP.from_date AS inizio_accoglienza,
                AP.to_date AS fine_accoglienza,
                AR.name AS motivo_uscita
            FROM person AS P
            JOIN acceptance_period AS AP ON P.id = AP.person_id
            JOIN service AS S ON AP.service_id = S.id
            JOIN nation_i18n AS N ON (P.birth_nation_id = N.nation_id AND N.locale='it_IT')
            JOIN acceptance_event AE ON AP.out_acceptance_event_id = AE.id
            JOIN acceptance_reason AS AR ON AE.acceptance_reason_id = AR.id

            WHERE (
                (
                    (P.name = %s AND P.surname = %s)
                    OR (P.name = %s AND P.surname = %s)
                )
                AND N.name = %s
                AND P.birth_date = %s
                AND P.gender = %s
                AND AP.from_date >= DATE_SUB(%s, INTERVAL 5 DAY)
            )
            LIMIT 0, 5
        """

        cursor.execute(sql,
            (
                p.id,
                p.name,
                p.surname,
                p.surname,
                p.name,
                p.birth_nation,
                p.birth_date,
                p.gender,
                p.from_date,
            )
        )

        result = cursor.fetchall()

        if result:
            dataframe["ant_uuid"].append(result[0]['ant_uuid'])
        else:
            dataframe["ant_uuid"].append(None)

        index = 1
        for r in result:
            dataframe[f"centro_{index}"].append(r['centro'])
            dataframe[f"ingresso_{index}"].append(r['inizio_accoglienza'])
            dataframe[f"uscita_{index}"].append(r['fine_accoglienza'])
            dataframe[f"motivazione_{index}"].append(r['motivo_uscita'])

            index += 1

        if index < 6:
            for n in range(index, 6):
                dataframe[f"centro_{n}"].append(None)
                dataframe[f"ingresso_{n}"].append(None)
                dataframe[f"uscita_{n}"].append(None)
                dataframe[f"motivazione_{n}"].append(None)
    
   
    print(dataframe)

    df_out = pd.DataFrame(dataframe)
    df_out.to_excel(writer, index=True, header=True)

    writer.save()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Processa gli xlsx della Banca Dati Centrale e produce un xlsx con i dettagli delle accoglienze.')
    parser.add_argument('-i', '--input_file', metavar='N', type=str,
                    help='File xlsx in ingresso', required=True)
    parser.add_argument('-o', '--output_file', metavar='N', type=str,
                    help='File xlsx in otput')

    args = parser.parse_args()
    input = args.input_file
    output = args.output_file if args.output_file else './xls/out.xlsx'
    
    main(input, output)