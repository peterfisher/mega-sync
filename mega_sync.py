""" Sync application for mega.co.nz

"""

__author__ = 'peter@phyn3t.com'

import sqlite3
import logging
import os
import sys
import ConfigParser
import ntpath
import re
import shutil
from mega_thing import mega_thing
import base64 as b64
from mega import Mega
from bucket import bucket

#Global Vars
CONFIG_PATH = None
LOCAL_SYNC = None
MEGA_OBJ = None
LOG_FILE = None
LOGGER = None
DB = None
MEGA_STORE = {}
INITIAL_STORE = {}

def load_config(CONFIG_PATH=None):
    """Load configuration file

        returns mega username and password in that order contained
        in a list
    """

    global LOCAL_SYNC
    global LOG_FILE

    config = ConfigParser.RawConfigParser()
    config.read(CONFIG_PATH)

    try:
        LOCAL_SYNC = config.get('main', 'local')
        LOG_FILE = config.get('main', 'log')
        user = config.get('main', 'user')
        passwd = config.get('main', 'passwd')
    except ConfigParser.Error as error:
        LOGGER.error("Problem loading config file, see\
        documentation\n" + str(error))
        sys.exit(3)

    return [user, passwd]


def mega_login(user=None, passwd=None):
    """ Login to mega.co.nz
    """

    global MEGA_OBJ

    try:
        mega = Mega({'verbose': True})
        MEGA_OBJ = mega.login(user, passwd)
    except Exception as error: #TODO: Find actual exception
        LOGGER.error('Failed to login to mega.co.nz '
                'check username or password.\n' + str(error))
        sys.exit(1)


def update_local_cache():
    """ Discover our local file system and add it to our sqlite db.
    """

    directories = [x[0] for x in os.walk(LOCAL_SYNC)]
    directories.pop(0)
    directories = [s.lstrip('.') for s in directories]

    DB.execute('delete from local') #TODO: make efficient

    for dirr in directories:
        DB.execute("INSERT or REPLACE INTO "
          "local values ('%s', 'D', '%s', '0');"
          %(dirr[len(LOCAL_SYNC)-1:], 'None'))

    for path, subdirs, files in os.walk(LOCAL_SYNC):
        for name in files:
            rel_path = os.path.join(path, name).lstrip('.')
            file_size = os.path.getsize(rel_path)
            md5 = "None"
            # path, type, md5, timestamp
            DB.execute("INSERT or REPLACE INTO "
                "local values ('%s', 'F', '%s', '%s');"
                %(rel_path[len(LOCAL_SYNC)-1:], md5,
                os.path.getmtime(os.path.join(path, name))))
    DB.commit()


def build_mega_store():
    """ Fetch all objects from mega.co.nz

        Sets global variable MEGA_STORE which is a dict.
        MEGA_STORE:
            key - the result of megaz.get_id()
            value - the result of megaz.get_obj()
            
    """

    global MEGA_STORE
    global INITIAL_STORE

    all_obj = MEGA_OBJ.get_files()
    INITIAL_STORE = all_obj

    for mega_dict in all_obj:
        megaz = mega_thing(all_obj[mega_dict])
        local_path = build_mega_path(megaz, 'absolute')
        if re.search(r'^/Cloud Drive/', local_path):
            MEGA_STORE[megaz.get_id()] = megaz.get_obj()


def update_mega_cache():
    """ Discover our mega.co.nz file system and add it to our sqlite db.
    """

    if MEGA_STORE is None:
        build_mega_store()

    DB.execute('DELETE from remote') #TODO: make efficient
    for item in MEGA_STORE:
        megaz = mega_thing(MEGA_STORE[item])
        path = build_mega_path(megaz)
        mega_id = megaz.get_id()
        # path, type, mega id, timestamp
        DB.execute("INSERT or REPLACE INTO "
                "remote values ('%s', '%s', '%s', '%s');"
                %(path, megaz.get_type(), mega_id, megaz.get_timestamp()))
    DB.commit()


def build_mega_path(megaz=None, style='relative'):
    """ Build a absolute file or folder path

        obj - mega_thing object
    """

    path = [ megaz.get_name()]

    while 1:
        try:
            megaz_parent = mega_thing(INITIAL_STORE[megaz.get_parent()])
        except KeyError:
            break
        path.append(megaz_parent.get_name())
        megaz = megaz_parent

    path.reverse()
    path = '/' + '/'.join(path)
    if style == 'relative':
        path = re.sub('Cloud Drive/', '', path)
    return path


def get_local():
    """ Build a set of all local files and directories
        return - set containing relative path of files and dir in stage

    """

    local = set()
    path_length = len(LOCAL_SYNC)-1

    directories = [x[0] for x in os.walk(LOCAL_SYNC)]
    directories.pop(0)
    directories = [s.lstrip('.')[path_length:] for s in directories]
    local.update(directories)

    for path, subdirs, files in os.walk(LOCAL_SYNC):
        for name in files:
            rel_path = os.path.join(path, name).lstrip('.')
            local.add(rel_path[path_length:])
    return local


def get_mega():
    """ Build a dict of all local files and directories in mega
    """

    mega_dict = {}

    for item in MEGA_STORE:
        megaz = mega_thing(MEGA_STORE[item])
        path = build_mega_path(megaz)
        mega_dict[path] = megaz.get_id()
    return mega_dict


def find_parent(path=None):
    """ Determine the mega.co.nz parent id for specified object
        path - local path to object
    """

    build_mega_store() #TODO: This should only be called when we need it
    path = path[len(LOCAL_SYNC)-1:]
    path = os.path.dirname(path)

    if re.match(r'^/[^/]+/$', path):
        return None

    for item in MEGA_STORE:
        megaz = mega_thing(MEGA_STORE[item])
        megaz_path = build_mega_path(megaz)
        if path == megaz_path:
            return megaz.get_id()


def download(mega_obj=None, path=None):
    """ Download our mega object
    """

    megaz = mega_thing(mega_obj)

    if megaz.get_type() == 'folder':
        path = LOCAL_SYNC + build_mega_path(megaz)
        os.makedirs(path)
        return
    MEGA_OBJ.download((0, mega_obj), path)


def upload(local_path=None, parent_id=None, name=None):
    """ Upload file or folder to mega

        local_path - local file or folder path
        parent_id - the mega.co.nz parent folder id
        name - name of the file or folder in mega.co.nz

    """

    if os.path.isfile(local_path):
        MEGA_OBJ.upload(local_path, parent_id, name)
    else:
        MEGA_OBJ.create_folder(name, parent_id)


def add_operation():
    """ Add any new objects
    """

    current_local = get_local()
    cached_local = DB.get_local()
    current_mega = get_mega()
    cached_mega = DB.get_remote()

    diff_local = list(current_local - cached_local)
    diff_local.sort()
    for item in diff_local:
        local_path = LOCAL_SYNC + item.lstrip('/')
        parent_id = find_parent(local_path)
        try:
            upload(local_path, parent_id, ntpath.basename(item))
        except Exception as err:
            LOGGER.error('Failed to upload new files to mega...')
            print str(err)
            sys.exit(2)

    diff_remote = set(current_mega.keys()) - cached_mega
    for obj_path in diff_remote:
        obj_id = current_mega[obj_path]
        local_path = LOCAL_SYNC + obj_path
        local_path = os.path.dirname(local_path)
        download(MEGA_STORE[obj_id], local_path) #TODO: If exist, delete it


def delete_operation():
    """ Delete any objects that have been deleted
    """

    current_mega = get_mega()
    cached_mega = DB.get_remote()
    current_local = get_local()
    cached_local = DB.get_local()

    diff_local = cached_local - current_local
    for obj_path in diff_local:
        path = obj_path
        obj_id = current_mega[path]
        MEGA_OBJ.destroy(obj_id)

    diff_mega = cached_mega - set(current_mega)
    for obj_path in diff_mega:
        path = LOCAL_SYNC.rstrip('/') + obj_path
        if os.path.isdir(path):
            try:
                shutil.rmtree(path)
            except OSError as err:
                logging.warn("Local dir couldn't be deleted: %s" %(path))
                print(str(err))
        else:
            try:
                os.remove(path)
            except OSError as err:
                logging.warn("Local file couldn't be deleted: %s" %(path))
                print(str(err))


def main():
    """ Main Control Function dude
    """

    global LOGGER
    global DB

    log_style = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=log_style, filename=LOG_FILE)
    LOGGER = logging.getLogger('mega_sync')
    LOGGER.info("Mega Sync started...")

    user,passwd = load_config('config')
    mega_login(user, passwd)

    DB = bucket('cache.db')
    build_mega_store()

    # Do our add and delete operations
    add_operation()
    delete_operation()

    # Update our caches so we have a difference for next run
    update_local_cache()
    update_mega_cache()

    #TODO: Modification to existing files...

    DB.close()

main()

