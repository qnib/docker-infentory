#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""

Usage:
    qnib_inventory.py [options]
    qnib_inventory.py (-h | --help)
    qnib_inventory.py --version

Options:
    --host <str>            neo4j hostname [default: neo4j.service.consul]
    --server                Start server listening for JSON blobs to lookup
    --zmq-host <str>        zmq host to bind to [default: 0.0.0.0]
    --zmq-port <int>        zmq socket to bind to [default: 5557]

General Options:
    -h --help               Show this screen.
    --version               Show version.
    --loglevel, -L=<str>    Loglevel [default: INFO]
                            (ERROR, CRITICAL, WARN, INFO, DEBUG)
    --log2stdout, -l        Log to stdout, otherwise to logfile. [default: False]
    --logfile, -f=<path>    Logfile to log to (default: <scriptname>.log)
    --cfg, -c=<path>        Configuration file.

"""

# load librarys
import logging
import os
import re
import codecs
import ast
import sys
import json
import time
from requests.exceptions import ConnectionError
from ConfigParser import RawConfigParser, NoOptionError

from neo4jrestclient.client import GraphDatabase, Node
from neo4jrestclient.query import QuerySequence
import yaml

try:
    from docopt import docopt
except ImportError:
    HAVE_DOCOPT = False
else:
    HAVE_DOCOPT = True
try:
    import zmq
except ImportError:
    HAVE_ZMQ = False
else:
    HAVE_ZMQ = True

__author__ = 'Christian Kniep <christian()qnib.org>'
__copyright__ = 'Copyright 2015 QNIB Solutions'
__license__ = """GPL v2 License (http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)"""


class QnibConfig(RawConfigParser):
    """ Class to abstract config and options
    """
    specials = {
        'TRUE': True,
        'FALSE': False,
        'NONE': None,
    }

    def __init__(self, opt):
        """ init """
        RawConfigParser.__init__(self)
        if opt is None:
            self._opt = {
                "--log2stdout": False,
                "--logfile": None,
                "--loglevel": "ERROR",
            }
        else:
            self._opt = opt
            self.loglevel = opt['--loglevel']
            self.logformat = '%(asctime)-15s %(levelname)-5s [%(module)s] %(message)s'
            self.log2stdout = opt['--log2stdout']
            if self.loglevel is None and opt.get('--cfg') is None:
                print "please specify loglevel (-L)"
                sys.exit(0)
            self.eval_cfg()

        self.eval_opt()
        self.set_logging()
        logging.info("SetUp of QnibConfig is done...")

    def do_get(self, section, key, default=None):
        """ Also lent from: https://github.com/jpmens/mqttwarn
            """
        try:
            val = self.get(section, key)
            if val.upper() in self.specials:
                return self.specials[val.upper()]
            return ast.literal_eval(val)
        except NoOptionError:
            return default
        except ValueError:  # e.g. %(xxx)s in string
            return val
        except:
            raise
            return val

    def config(self, section):
        ''' Convert a whole section's options (except the options specified
                explicitly below) into a dict, turning

                    [config:mqtt]
                    host = 'localhost'
                    username = None
                    list = [1, 'aaa', 'bbb', 4]

                into

                    {u'username': None, u'host': 'localhost', u'list': [1, 'aaa', 'bbb', 4]}

                Cannot use config.items() because I want each value to be
                retrieved with g() as above
            SOURCE: https://github.com/jpmens/mqttwarn
            '''

        d = None
        if self.has_section(section):
            d = dict((key, self.do_get(section, key))
                     for (key) in self.options(section) if key not in ['targets'])
        return d

    def eval_cfg(self):
        """ eval configuration which overrules the defaults
            """
        cfg_file = self._opt.get('--cfg')
        if cfg_file is not None:
            fd = codecs.open(cfg_file, 'r', encoding='utf-8')
            self.readfp(fd)
            fd.close()
            self.__dict__.update(self.config('defaults'))

    def eval_opt(self):
        """ Updates cfg according to options """

        def handle_logfile(val):
            """ transforms logfile argument
                """
            if val is None:
                logf = os.path.splitext(os.path.basename(__file__))[0]
                self.logfile = "%s.log" % logf.lower()
            else:
                self.logfile = val

        self._mapping = {
            '--logfile': lambda val: handle_logfile(val),
        }
        for key, val in self._opt.items():
            if key in self._mapping:
                if isinstance(self._mapping[key], str):
                    self.__dict__[self._mapping[key]] = val
                else:
                    self._mapping[key](val)
                break
            else:
                if val is None:
                    continue
                mat = re.match("\-\-(.*)", key)
                if mat:
                    self.__dict__[mat.group(1)] = val
                else:
                    logging.info("Could not find opt<>cfg mapping for '%s'" % key)

    def set_logging(self):
        """ sets the logging """
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.DEBUG)
        if self.log2stdout:
            hdl = logging.StreamHandler()
            hdl.setLevel(self.loglevel)
            formatter = logging.Formatter(self.logformat)
            hdl.setFormatter(formatter)
            self._logger.addHandler(hdl)
        else:
            hdl = logging.FileHandler(self.logfile)
            hdl.setLevel(self.loglevel)
            formatter = logging.Formatter(self.logformat)
            hdl.setFormatter(formatter)
            self._logger.addHandler(hdl)

    def __str__(self):
        """ print human readble """
        ret = []
        for key, val in self.__dict__.items():
            if not re.match("_.*", key):
                ret.append("%-15s: %s" % (key, val))
        return "\n".join(ret)

    def __getitem__(self, item):
        """ return item from opt or __dict__
        :param item: key to lookup
        :return: value of key
        """
        if item in self.__dict__.keys():
            return self.__dict__[item]
        else:
            return self._opt[item]

class QNIBInv(object):
    """ Class to hold the functionality of the script
    """

    def __init__(self, cfg):
        """ Init of instance
        """
        self._cfg = cfg
        self.con_gdb()

        self._labels = {
            "SW_BASE": self._gdb.labels.create("SW_BASE"),
            "SYS": self._gdb.labels.create("SYSTEM"),
        }

    def con_gdb(self):
        """ connect to neo4j
        """
        url = "http://%(--host)s:7474" % self._cfg
        try:
            self._gdb = GraphDatabase(url)
        except ConnectionError:
            time.sleep(3)
            self.con_gdb()

    def run(self):
        """ runs the business code
        """
        if self._cfg['--server']:
            if not HAVE_ZMQ:
                self._cfg._logger.error("No zmq library found. Please install python-zmq...")
                sys.exit(1)
            self.run_server()

    def run_server(self):
        """ Run loop to serve zmq socket for inventory lookup
        """
        context = zmq.Context()
        self._consumer_receiver = context.socket(zmq.REP)
        url = "tcp://%(--zmq-host)s:%(--zmq-port)s" % self._cfg
        self._cfg._logger.info("Connect to '%s'" % url)
        self._consumer_receiver.bind(url)
        while True:
            msg = json.loads(self._consumer_receiver.recv())
            new_msg = self.lookup_inv(msg)
            self._consumer_receiver.send(json.dumps(new_msg))

    def lookup_inv(self, msg):
        """ Looks up information within the GraphDB
        :param msg: JSON blob from logstash
        :return: enriched JSON blob to reply to logstash
        """
        msg['inventory_lookup'] = 1
        if 'program' not in msg.keys():
            msg['no_program'] = 1
        elif msg['program'] == "slurmd" and msg['message'].startswith("launch task "):
            regex = "launch\s+task\s+(?P<jobid>\d+)\.(?P<task_nr>\d+)\s+request\s+from\s+"
            regex += "(?P<userid>\d+)\.(?P<groupid>\d+)@(?P<ip_addr>[\w\.]+)\s+\(port\s+(?P<port_nr>\d+)\)"
            mat = re.match(regex, msg['message'])
            if mat:
                dic = mat.groupdict()
                msg['slurm_jobid'] = dic['jobid']
                msg['slurm_task'] = dic['task_nr']
        elif re.match("^slurm_\d+$", msg['program']):
            # Log message from jobscript
            dic = re.match("^slurm_(?P<jobid>\d+)$", msg['program']).groupdict()
            msg['program'] = "slurm_out"
            msg['slurm_jobid'] = dic['jobid']
        else:
            #msg['message'] = "NOT_HANDLED_BY_INVENTORY_YET: %(message)s" % msg
            msg['inventory_lookup'] = 0
        return msg

    def set_attr(self, msg, key, val):
        """ if key does not exists or is equal everything is fine, otherwise set severity to ERROR
        :param msg: JSON blob to update
        :param key: key to set
        :param val: val to set
        :return: JSON blob
        """
        if key not in msg.keys():
            msg[key] = val
            return msg
        elif key in msg.keys() and msg[key] == val:
            return msg
        else:
            # OHOH!
            msg['severity'] = 4
            msg['severity_label'] = 'Warn'
            msg['description'] = "Told to set %s:%s, but '%s' is already set to '%s'" % (key,val, key, msg[key])
            return msg

    def unfold(self, res):
        if isinstance(res, QuerySequence) and len(res) == 1:
            return res[0][0]
        if isinstance(res, list):
            ret = res.pop()
            self.unfold(ret)
        else:
            if isinstance(res, QuerySequence):
                return None
            return res

    def close(self):
        """ close connections
        """
        self._consumer_receiver.close()
        self._cfg._logger.info("gracefully ended server")

def main():
    """ main function """
    options = None
    if HAVE_DOCOPT:
        options = docopt(__doc__, version='1.0.1')
    qcfg = QnibConfig(options)
    qinv = QNIBInv(qcfg)
    try:
        qinv.run()
    except KeyboardInterrupt:
        qinv.close()

if __name__ == "__main__":
    main()
