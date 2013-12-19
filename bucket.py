""" Handles our sqlite3 database

    cause containing everything in its own class is more Feng shui babe

"""

__author__ = 'peter@phyn3t.com'

import sqlite3
import logging
import re
import os
import base64

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
            self.logger.debug(sql)
            result = self.db.execute(sql)

        except sqlite3.OperationalError as err:
            print str(err)
            regex = r'(?:sqlite3.OperationalError: near).*'
            error_info = re.match(regex, str(err)).group(0) #danger
            logger.error('Invalid SQL syntax near: ' + error_info)
            return False

        return result

    def put_cache(self, path=None, ftype=None, mega_id=None, timestamp=None):
        """ Update our local caches
            path - relative file path
            ftype - string either: (file, folder)
            mid - mega id
            timestamp - string epoch ref point

        """

        row_tuple = (path, ftype, mega_id, timestamp)

        self.logger.debug("Cache Path: " + path)
        self.insert_local(row_tuple)
        self.insert_remote(row_tuple)


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
            path_decoded = base64.b64decode(row[0])
            row_set.add(path_decoded)
        return row_set

    def get_remote(self):
        """ Get remote objects and return them in a set
        """

        row_set = set()
        remote_rows = self.execute('SELECT path from remote').fetchall()
        for row in remote_rows:
            path_decoded = base64.b64decode(row[0])
            row_set.add(path_decoded)
        return row_set

    def get_diff(self):
        """ Get data used for finding diff between local and remote.
            Return list containing tuple with file and timestamp
        """

        diff_report = [{}, {}]

        remote_rows = self.execute('SELECT path,timestamp from remote')
        for row in remote_rows:
            diff_report[0][base64.b64decode(row[0])] = row[1]
        local_rows = self.execute('SELECT path,timestamp from local')
        for row in local_rows:
            diff_report[1][base64.b64decode(row[0])] = row[1]

        return diff_report

    def insert_local(self, row=None):
        """ Handle inserts into our local cache.

            row - a tuple containing the following:
                * path - string with file path
                * ftype - string 'file' or 'folder'
                * mega_id - mega object id
                * timestamp - time since EPOCH

        """

        self.execute("INSERT or REPLACE INTO "
                "local values (\"%s\", '%s', '%s', '%s');"
                %(base64.standard_b64encode(row[0]), row[1], row[2],
                    row[3]))
        self.commit()

    def insert_remote(self, row=None):
        """ Handle inserts into our remote cache.

            row - a tuple containing the following:
                * path - string with file path
                * ftype - string 'file' or 'folder'
                * mega_id - mega object id
                * timestamp - time since EPOCH

        """

        self.execute("INSERT or REPLACE INTO "
            "remote values ('%s', '%s', '%s', '%s');"
            %(base64.standard_b64encode(row[0]), row[1], row[2],
                row[3]))
        self.commit()



