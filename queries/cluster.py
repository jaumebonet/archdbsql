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

def get_cluster_extended_loop_species_size(db, cluster, taxID, nid = None):
    if cluster.lower() == 'class':      query = 'cs.class_nid'
    if cluster.lower() == 'subclass':   query = 'cs.nid'

    db.select(query)
    db.table('cluster_subclass cs')
    db.join('loop2cluster l2c',  'l2c.cluster_nid = cs.nid'  )
    db.join('loop2chain lc',     'lc.loop_id = l2c.loop_nid' )
    db.join('chain2uniprot c2u', 'c2u.chain = lc.chain AND lc.start >= c2u.start AND lc.end <= c2u.end')
    db.join('uniprot2taxid u2t', 'u2t.uniprot = c2u.uniprot AND u2t.taxid = {0}'.format(taxID))
    db.where_in('lc.assignation',   ['D','H']                )
    if nid is not None:
        if   isinstance(nid, int):  db.where('nid', nid)
        elif isinstance(nid, list): db.where_in('nid', nid)
    db.group_by('{0}, lc.loop_id'.format(query))
    db.get()
    return Counter([x[0] for x in db.result()])

