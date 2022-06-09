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

load_dotenv()

MYSQL_DB = os.getenv("MYSQL_DB", "printer_counter")
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
        
def main():
    persons = []
    df = pd.read_excel(
        './xls/sanbenedetto_SAI.xlsx',
    )
    df = df.replace([np.nan], [None])
    as_dict = df.to_dict('records')

    for row in as_dict:
        persons.append(PersonAcceptance.import_row(row))

    writer = pd.ExcelWriter('./xls/out.xlsx', engine='xlsxwriter')

    cursor = db.cursor()

    write_header = True

    for p in persons:
        sql = """
            SELECT
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
                
            )
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
            )
        )

        result = cursor.fetchall()
        df_out = pd.DataFrame(result)

        if not write_header:
            reader = pd.read_excel(r'./xls/out.xlsx')
            df_out.to_excel(writer, index=False, header=False, startrow=len(reader)+1)
        else:
            df_out.to_excel(writer, index=True, header=True)
            write_header = False

        writer.save()

if __name__ == '__main__':
    main()