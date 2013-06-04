#!/usr/bin/python
from pymongo import MongoClient
import optparse
import pymongo
import util
import json

def print_oplog_last_ts(host, ns, destination, user=None, password=None):
    """Simply just print into a file the last timestamp
    """
    #connect to the target
    mc = MongoClient( host )
    if password is not None:
        mc['admin'].authenticate(user, password)
    local = mc['local']
    oplog = local['oplog.rs']
    last_oplog_entry = oplog.find_one({'ns': ns},
                                        sort=[('$natural',
                                        pymongo.DESCENDING)])
    with open(destination, 'w') as dest_file:
        ts = last_oplog_entry['ts']
        oplog_str = str(oplog)
        timestamp = util.bson_ts_to_long(ts)
        json_str = json.dumps([oplog_str, timestamp])
        try:
            dest_file.write(json_str)
            print json_str
            dest_file.close()
        except IOError:
            dest_file.truncate()
            print "Problem writing to file %s" % destination



if __name__ == '__main__':
    parser = optparse.OptionParser()
    #-o is to specify the oplog-config file. This file is used by the system
    #to store the last timestamp read on a specific oplog. This allows for
    #quick recovery from failure.
    parser.add_option("-o", "--oplog-ts", action="store", type="string",
                      dest="oplog_config", default="config.txt",
                      help="""Specify the name of the file that stores the"""
                      """oplog progress timestamps. """
                      """This file is used by the system to store the last"""
                      """timestamp read on a specific oplog. This allows"""
                      """ for quick recovery from failure. By default this"""
                      """ is `config.txt`, which starts off empty. An empty"""
                      """ file causes the system to go through all the mongo"""
                      """ oplog and sync all the documents. Whenever the """
                      """cluster is restarted, it is essential that the """
                      """oplog-timestamp config file be emptied - otherwise"""
                      """ the connector will miss some documents and behave"""
                      """incorrectly.""")

    #-t is to specify the URL to the target system being used.
    parser.add_option("-t", "--target-url", action="store", type="string",
                      dest="target", default=None,
                      help="""Target replicaset where to crop shard environment oplog
                      """)

    #-n is to specify the namespaces we want to consider. The default
    #considers all the namespaces
    parser.add_option("-n", "--namespace-set", action="store", type="string",
                      dest="ns_set", default=None, help=
                      """Used to specify the namespaces we want to """
                      """ consider. For example, if we wished to store all """
                      """ documents from the test.test and alpha.foo """
                      """ namespaces, we could use `-n test.test,alpha.foo`."""
                      """ The default is to consider all the namespaces, """
                      """ excluding the system and config databases, and """
                      """ also ignoring the "system.indexes" collection in """
                      """any database.""")
    #-p is to specify the password used for authentication.
    parser.add_option("-p", "--password", action="store", type="string",
                      dest="password", default=None, help=
                      """ Used to specify the password."""
                      """ This is used by mongos to authenticate"""
                      """ connections to the shards, and in the"""
                      """ oplog threads. If authentication is not used, then"""
                      """ this field can be left empty as the default """)

    #-a is to specify the username for authentication.
    parser.add_option("-a", "--admin-username", action="store", type="string",
                      dest="admin_name", default="__system", help=
                      """Used to specify the username of an admin user to"""
                      """authenticate with. To use authentication, the user"""
                      """must specify both an admin username and a keyFile."""
                      """The default username is '__system'""")
    (options, args) = parser.parse_args()
    print_oplog_last_ts( options.target, options.ns_set, options.oplog_config,
            options.admin_name, options.password)




