# snapbauer
Snapshot scheduler for pools in a Ceph cluster

This script was built to be executed in the crontab all ceph nodes simultaneously.

Commonly, an automatically snapshot schedule will be added to crontab of all ceph cluster nodes, which could cause some duplicity of snapshots once the schedule runs at the same time.

This script avoid that, because uses a native lock implementation of librados allowing you to add crontab line in all nodes.