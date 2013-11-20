""" mega.co.nz object class

"""

__author__ = 'peter@phyn3t.com'

import logging
from mega import Mega

class mega_thing:

    def __init__(self, mega=None):
        """instantiate our mega object
        """

        mega_types = [0, 1, 2, 3, 4]

        if not mega.has_key('t') or mega['t'] not in mega_types:
            print('Passing invalid object type')
            print mega['t']
            raise ValueError

        if mega['t'] == 0:
            self._init_file(mega)
        if mega['t'] == 1:
            self._init_folder(mega)
        if mega['t'] == 2:
            self._init_root(mega)
        if mega['t'] == 3:
            self._init_inbox(mega)
        if mega['t'] == 4:
            self._init_trash(mega)

        self.mega_obj = mega


    def _init_file(self, mega=None):
        """Instantiate a file mega object
        """

        self.ident = mega['h']
        self.name = mega['a']['n']
        self.node_key = mega['k']
        self.timestamp = mega['ts']
        self.iv = mega['iv']
        self.parent = mega['p']
        self.size = mega['s']
        self.meta_mac = mega['meta_mac']
        self.user_id = mega['u']
        self.obj_type = 'file'
        self.key = mega['key']


    def _init_folder(self, mega=None):
        """Instantiate a folder mega object
        """

        self.ident = mega['h']
        self.name = mega['a']['n']
        self.node_key = mega['k']
        self.timestamp = mega['ts']
        self.iv = None
        self.parent = mega['p']
        self.size = None
        self.meta_mac = None
        self.user_id = mega['u']
        self.obj_type = 'folder'
        self.key = mega['key']


    def _init_root(self, mega=None):
        """Instantiate a 'cloud drive' root folder
        """

        self.ident = mega['h']
        self.name = mega['a']['n']
        self.node_key = mega['k']
        self.timestamp = mega['ts']
        self.iv = None
        self.parent = mega['p']
        self.size = None
        self.meta_mac = None
        self.user_id = mega['u']
        self.obj_type = 'root'
        self.key = None


    def _init_inbox(self, mega=None):
        """Instantiate the inbox
        """

        self.ident = mega['h']
        self.name = mega['a']['n']
        self.node_key = mega['k']
        self.timestamp = mega['ts']
        self.iv = None
        self.parent = mega['p']
        self.size = None
        self.meta_mac = None
        self.user_id = mega['u']
        self.obj_type = 'inbox'
        self.key = None


    def _init_trash(self, mega=None):
        """Instantiate the trash
        """

        self.ident = mega['h']
        self.name = mega['a']['n']
        self.node_key = mega['k']
        self.timestamp = mega['ts']
        self.iv = None
        self.parent = mega['p']
        self.size = None
        self.meta_mac = None
        self.user_id = mega['u']
        self.obj_type = 'trash'
        self.key = None


    def get_id(self):
        """ Return object id
        """
        return self.ident

    def get_type(self):
        """ Return object type (file, folder, etc)
        """
        return self.obj_type

    def get_name(self):
        """ Return object name
        """
        return self.name

    def get_nodekey(self):
        """ Return object node key
        """
        return self.node_key

    def get_timestamp(self):
        """ Return object last modification time in seconds from EPOC
        """
        return self.timestamp

    def get_iv(self):
        """ Return initialization vectors
        """
        return self.iv

    def get_parent(self):
        """ Return parent object id
        """
        return self.parent

    def get_size(self):
        """ Return size of object in bytes
        """
        return self.size

    def get_metamac(self):
        """ Return metamac
        """
        return self.meta_mac

    def get_uid(self):
        """ Return user id
        """
        return self.user_id

    def get_key(self):
        """ Return object key
        """
        return self.key

    def get_obj(self):
        """ Return mega object
        """
        return self.mega_obj
