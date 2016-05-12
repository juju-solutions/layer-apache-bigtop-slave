# pylint: disable=unused-argument
from charms.reactive import when_any, when_none, is_state
from charmhelpers.core.hookenv import status_set


@when_none('namenode.spec.mismatch', 'resourcemanager.spec.mismatch')
@when_any(
    'bigtop.available',
    'apache-bigtop-datanode.started',
    'apache-bigtop-nodemanager.started',
    'namenode.joined',
    'namenode.ready',
    'resourcemanager.joined',
    'resourcemanager.ready',
)
def update_status():
    hdfs_rel = is_state('namenode.joined')
    yarn_rel = is_state('resourcemanager.joined')
    hdfs_ready = is_state('namenode.ready')
    yarn_ready = is_state('resourcemanager.ready')

    if not (hdfs_rel or yarn_rel):
        status_set('blocked',
                   'missing required namenode and/or resourcemanager relation')
    elif hdfs_rel and not hdfs_ready:
        status_set('waiting', 'waiting for hdfs to become ready')
    elif yarn_rel and not yarn_ready:
        status_set('waiting', 'waiting for yarn to become ready')
    else:
        ready = []
        if hdfs_ready:
            ready.append('datanode')
        if yarn_ready:
            ready.append('nodemanager')
        status_set('active', 'ready ({})'.format(' & '.join(ready)))
