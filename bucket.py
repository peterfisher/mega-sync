""" Handles our sqlite3 database

    cause containing everything in its own class is more Feng shui babe

"""

__author__ = 'peter@phyn3t.com'

import sqlite3
import logging
import re
import os

class bucket:

    def __init__(self, db_path='cache.db'):
        """ Instantiate our db object

            db_path = Path to the sqlite3 database file

        """

        self.db = None
        self.db_conn = None
        self.db_path = db_path

        log_style = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(format=log_style, filename='sqlite_thing.log')
        self.logger = logging.getLogger('mega_sync')

        self._check_database()
        self._db_initialize()


    def _check_database(self):
        """ Check to make sure our database exist and is populated
        """

        if not os.path.isfile(self.db_path):
            self._db_initialize()
            self._create_db()
        else:
            self._db_initialize()


    def _db_initialize(self):
        """ Setup the connection to our local database.
        """

        try:
            self.db_conn = sqlite3.connect(self.db_path)
            self.db = self.db_conn.cursor()

        except IOError as error:
            self.logger.error('Failed to create database file')
            sys.exit(2)


    def _create_db(self):
        """ Create our local sqlite3 database
        """

        self.db.execute('''CREATE TABLE metadata(\
                initialized INTEGER)''')
        self.db.execute('''CREATE TABLE local\
                (path TEXT PRIMARY KEY, type TEXT,\
                md5 TEXT, timestamp TEXT)''')
        self.db.execute('''CREATE TABLE remote (path TEXT PRIMARY KEY,\
                type TEXT, md5 TEXT, timestamp TEXT)''')

        self.db_conn.commit()
        self.db_conn.close()


    def execute(self, sql=None):
        """ Execute SQL on the database
        """

        try:
            result = self.db.execute(sql)

        except sqlite3.OperationalError as error:
            regex = r'(?:sqlite3.OperationalError: near).*'
            error_info = re.match(regex, str(error)).group(0) #danger
            logger.error('Invalid SQL syntax near: ' + error_info)
            return False

        return result


    def commit(self):
        """ Commit changes to the databse
        """
        self.db_conn.commit()


    def close(self):
        """ Close our connection to the database
        """
        self.db_conn.close()

    def get_local(self):
        """ Get local objects and return them in a set
        """

        row_set = set()
        local_rows = self.execute('SELECT path from local').fetchall()
        for row in local_rows:
            row_set.add(row[0])
        return row_set

    def get_remote(self):
        """ Get remote objects and return them in a set
        """

        row_set = set()
        remote_rows = self.execute('SELECT path from remote').fetchall()
        for row in remote_rows:
            row_set.add(row[0])

        return row_set



