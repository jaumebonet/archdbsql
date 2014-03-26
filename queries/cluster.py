from collections import Counter
def get_cluster_size(db, cluster, nid = None):
    db.select('nid, size')
    if cluster.lower() == 'class':      db.table('cluster_class')
    if cluster.lower() == 'subclass':   db.table('cluster_subclass')
    if nid is not None:
        if   isinstance(nid, int):  db.where('nid', nid)
        elif isinstance(nid, list): db.where_in('nid', nid)
    db.get()
    return dict(db.result())

def get_cluster_loopsource_size(db, cluster, nid = None):
    if cluster.lower() == 'class':      query = 'cs.class_nid'
    if cluster.lower() == 'subclass':   query = 'cs.nid'

    db.select(query)
    db.table('cluster_subclass cs')
    db.join('loop2cluster l2c', 'l2c.cluster_nid = cs.nid'  )
    db.join('loop2chain lc',    'lc.loop_id = l2c.loop_nid' )
    db.where('lc.assignation',  'D'                         )
    if nid is not None:
        if   isinstance(nid, int):  db.where('nid', nid)
        elif isinstance(nid, list): db.where_in('nid', nid)
    db.group_by('{0}, lc.chain'.format(query))
    db.get()
    return Counter([x[0] for x in db.result()])
