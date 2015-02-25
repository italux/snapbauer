#!/usr/bin/python

import sys
import rados
import syslog
from datetime import datetime
from optparse import OptionParser

class Snapshot(object):
    """docstring for Snapshot"""
    def __init__(self, cluster):
        super(Snapshot, self).__init__()
        self.cluster = cluster
        cluster.connect()
        self.lock = "snapshot.lock"
        
    def create(self, pool):

        if self.cluster.pool_exists(pool):
            ioctx = self.cluster.open_ioctx(pool)
        else:
            raise Exception('ERROR - pool "%s" does not exists. Abort!' % pool)
        
        ioctx.lock_exclusive(self.lock, 'snapshot_lock', 'SnapCookie', '', 0)

        try:
            ioctx.create_snap('%s@%s' % (pool, datetime.now().strftime('%Y%M%d-%H%m%S')))
        finally:
            ioctx.unlock(self.lock, 'snapshot_lock', 'SnapCookie')
            ioctx.remove_object(self.lock)

if __name__ == "__main__":
    usage = "%prog [options] args"
    parser = OptionParser(usage)
    parser.add_option("-p", "--pool", dest="pool", help="Pool name")
    parser.add_option("-c", "--config", dest="config", help="Ceph configuration file path", default="/etc/ceph/ceph.conf")
    (options, args) = parser.parse_args()
    
    if options.pool is None or options.config is None:
        print("Incorrect arguments numbers.\n")
        parser.print_help()
        sys.exit(-1)
    
    pool = options.pool
    config = options.config
    cluster = rados.Rados(conffile=config)

    try:
        snap = Snapshot(cluster)
        snap.create(pool)
    except rados.ObjectBusy:
        syslog.syslog(syslog.LOG_NOTICE, 'snapbauer: %s is alredy locked by another node.' % lock_obj)
        print 'ERROR: %s is alredy locked by another node.' % lock_obj
    except rados.ObjectNotFound:
        syslog.syslog(syslog.LOG_NOTICE, 'snapbauer: object %s does not exists anymore, why?' % lock_obj)
        print 'ERROR: Object %s does not exists anymore, why?' % lock_obj
    except Exception, e:
        syslog.syslog(syslog.LOG_NOTICE, 'snapbauer: ERROR - Failed to create Snapshot on %s. Exception! "%s"' % (pool,e))
        print 'ERROR - Failed to create Snapshot on %s. Abort!' % pool
        sys.exit(1)
