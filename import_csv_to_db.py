import logging

from app import db
from manage import manager
from sqlalchemy import create_engine

#
# Import CSV to development database EXCEPT first row
# - It will import all CSV rows except first row ( HEADER )
#
@manager.command
def import_csv_to_dev_db(table='', file_name=''):
    if not table or not file_name:
        return 'Error: table name or file name is empty!'

    logging.info('### STARTING DB IMPORT table: ' + table + ' from file: data/' + file_name + ' ###')
    with open('./data/' + file_name, 'r') as f:
        # Skip the header row
        next(f)
        conn = db.engine.raw_connection()
        cursor = conn.cursor()
        cursor.copy_expert('COPY ' + table + ' FROM STDIN WITH (FORMAT CSV)', f)
    conn.commit()
    logging.info('### FINISHED DB IMPORT ###')
    pass
