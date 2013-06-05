#!/usr/bin/python
from pymongo import MongoClient
import optparse
import pymongo
import util
import json

def get_shard_nodes(host):
    mc = MongoClient( host )
    shard_coll = mc['config']['shards']
    shard_cursor = shard_coll.find()
    shards = [ x for x in shard_cursor ]
    mc.disconnect()
    return shards

def validate_destination_file(destination):
        source = open(destination, 'r')
        try:
            data = json.load(source)
        except ValueError, e:       # empty file
            import ipdb;ipdb.set_trace()
            err_msg = "MongoConnector: Can't read oplog progress file."
            reason = "It may be empty or corrupt."
            print("%s %s %s" % (err_msg, reason, e))
        source.close()
        print "OK"


def print_oplog_last_ts(host, origin, ns, destination, user=None, password=None):
    """Simply just print into a file the last timestamp
    """
    #newst oplog entry
    newst_oplog = 0

    #get all shards from target url
    target_shards = get_shard_nodes(host)
    #go to all nodes of the shard and get the newst oplog operation.
    for shard_doc in target_shards:
        repl_set, hosts = shard_doc['host'].split('/')
        shard_connection = MongoClient( hosts, replicaSet=repl_set )

        local = shard_connection['local']
        oplog = local['oplog.rs']
        #collect the last timestamp for the oplog which is not a migration operation
        last_oplog_entry = oplog.find_one({'ns': ns, 'fromMigrate':{'$exists':0}},
                                        sort=[('$natural',
                                        pymongo.DESCENDING)])
        #check if the is a last entry
        timestamp = util.bson_ts_to_long( last_oplog_entry['ts'] )
        if timestamp > newst_oplog:
            newst_oplog = timestamp

    #get all shards from target url
    origin_shards = get_shard_nodes(origin)


    with open(destination, 'w') as dest_file:
        jsons = []
        for shard_doc in origin_shards:
            repl_set, hosts = shard_doc['host'].split('/')
            shard_connection = MongoClient( hosts, replicaSet=repl_set )

            local = shard_connection['local']
            oplog = local['oplog.rs']

            oplog_str = str(oplog)
            jsons.append( [oplog_str, newst_oplog] )
        try:
            json_str = json.dumps(jsons)
            dest_file.write(json_str)
            print json_str
        except IOError:
            dest_file.truncate()
            print "Problem writing to file %s" % destination

    dest_file.close()
    validate_destination_file(destination)


if __name__ == '__main__':
    parser = optparse.OptionParser()
    #-m is for the main address, which is a host:port pair, ideally of the
    #mongos. For non sharded clusters, it can be the primary.
    parser.add_option("-m", "--main", action="store", type="string",
                      dest="main_addr", default="localhost:27217",
                      help="""Specify the main address, which is a"""
                      """ host:port pair. For sharded clusters,  this"""
                      """ should be the mongos address. For individual"""
                      """ replica sets, supply the address of the"""
                      """ primary. For example, `-m localhost:27217`"""
                      """ would be a valid argument to `-m`. Don't use"""
                      """ quotes around the address""")
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
    print_oplog_last_ts( options.target, options.main_addr, options.ns_set, options.oplog_config,
            options.admin_name, options.password)




