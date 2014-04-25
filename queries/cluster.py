from collections import Counter
import re
import method


def get_cluster_size(db, cluster, nid=None):
    db.select('nid, size')
    if cluster.lower() == 'class':
        db.table('cluster_class')
    if cluster.lower() == 'subclass':
        db.table('cluster_subclass')
    if nid is not None:
        if isinstance(nid, int):
            db.where('nid', nid)
        elif isinstance(nid, list):
            db.where_in('nid', nid)
    db.get()
    return dict(db.result())


def get_subclass_from_geometries_range(db, methodID, ctrtype, ctrlength,
                                       distance, theta, rho, delta,
                                       length_range=0, dist_range=0,
                                       theta_range=0, rho_range=0,
                                       delta_range=0):
    results = []

    if isinstance(methodID, int):
        method_nid  = methodID
        method_name = method.get_method_name(db, methodID)
    else:
        method_nid  = method.get_method_nid(db, methodID)
        method_name = methodID

    if length_range == -1:
        length_range = 10000

    db.select('CONCAT("{0}.",cs.name), cs.nid'.format(method_name))
    db.select('cc.length, cs.rho_range_min, cs.rho_range_max')
    db.table('cluster_subclass cs')
    db.join('cluster_class cc', 'cs.class_nid=cc.nid')
    db.where('cc.method', method_nid)
    db.where('cc.type',   ctrtype)
    mdist, Mdist = 'cs.dist_range_min', 'cs.dist_range_max'
    db.where('{1} - {0} <='.format(dist_range, mdist),   str(distance))
    db.where('{1} + {0} >='.format(dist_range, Mdist),   str(distance))
    mtheta, Mtheta = 'cs.theta_range_min', 'cs.theta_range_max'
    db.where('{1} - {0} <='.format(theta_range, mtheta), str(theta))
    db.where('{1} + {0} >='.format(theta_range, Mtheta), str(theta))
    mdelta, Mdelta = 'cs.delta_range_min', 'cs.delta_range_max'
    db.where('{1} - {0} <='.format(delta_range, mdelta), str(delta))
    db.where('{1} + {0} >='.format(delta_range, Mdelta), str(delta))
    db.get()
    for row in db.result():
        search_length = re.search('(\d+)\w*', row[2])
        query_length  = re.search('(\d+)\w*', str(ctrlength))
        ctrlength     = int(query_length.group(1))
        minlrange = int(search_length.group(1)) - length_range
        maxlrange = int(search_length.group(1)) + length_range
        if int(ctrlength) >= minlrange and int(ctrlength) <= maxlrange:
            minrho = float(row[3]) - rho_range
            maxrho = float(row[4]) + rho_range
            if float(row[4]) - float(row[3]) > 100:
                if rho >= maxrho and rho <= minrho:
                    results.append(tuple(row[:2]))
            else:
                if rho >= minrho and rho <= maxrho:
                    results.append(tuple(row[:2]))

    return results


def get_similar_subclasses_to(db, subclass_nid, methodID, length_range,
                              dist_range, theta_range, rho_range, delta_range):

    db.select('dist_range_min, dist_range_max, delta_range_min, delta_range_max')
    db.select('rho_range_min, rho_range_max, theta_range_min, theta_range_max')
    db.select('type, length')
    db.table('cluster_subclass')
    db.join('cluster_class', 'cluster_subclass.class_nid = cluster_class.nid')
    db.where('cluster_subclass.nid', subclass_nid)
    db.get()
    result = db.result()[0]
    return get_subclass_from_geometries_range(db, methodID, result[8], result[9],
                                              result[0]+result[1]/float(2),
                                              result[6]+result[7]/float(2),
                                              result[4]+result[5]/float(2),
                                              result[2]+result[3]/float(2),
                                              length_range, dist_range,
                                              theta_range, rho_range, delta_range)


def get_subclass_contacts(db, nid, contact_type):
    if contact_type == 'sites':
        return get_subclass_sites(db, nid)
    if contact_type == 'ligands':
        return get_subclass_ligands(db, nid)


def get_subclass_sites(db, nid):
    db.select('c.pdb, s.name, s.description, h.id, h.name')
    db.table('loop2cluster lc')
    db.join('loop2chain l2c', 'l2c.loop_id=lc.loop_nid')
    db.join('chain c', 'c.nid=l2c.chain')
    db.join('site2position s2p', 's2p.chain=l2c.chain ' +
            'AND s2p.position>=l2c.start AND s2p.position<=l2c.end')
    db.join('site s', 's.nid=s2p.site')
    db.join('hetero2PDB h2p', 'h2p.nid=s.bind')
    db.join('hetero h', 'h.id=h2p.hetero')
    db.where('l2c.assignation', 'D')
    if isinstance(nid, list):
        db.where_in('lc.cluster_nid', nid)
    else:
        db.where('lc.cluster_nid', nid)
    db.group_by('c.pdb, s.name')
    db.get()

    s = {}
    for row in db.result():
        s.setdefault(row[3], {'name': row[3], 'long': row[4], 'source': [],
                              'description': 'BINDING SITE FOR {0}'.
                              format(row[3])})
        s[row[3]]['source'].append(tuple(row[:2]))
    data = []
    for k, v in s.iteritems():
        data.append(v)
    return data


def get_subclass_ligands(db, nid):
    db.select('c.pdb, h.id, h.name')
    db.table('loop2cluster lc')
    db.join('loop2chain l2c', 'l2c.loop_id=lc.loop_nid')
    db.join('chain c', 'c.nid=l2c.chain')
    db.join('hetero_contacts6 hc6', 'hc6.chain=l2c.chain ' +
            'AND hc6.position>=l2c.start AND hc6.position<=l2c.end')
    db.join('hetero2PDB h2p', 'h2p.nid=hc6.hetero')
    db.join('hetero h', 'h.id=h2p.hetero')
    db.where('l2c.assignation', 'D')
    if isinstance(nid, list):
        db.where_in('lc.cluster_nid', nid)
    else:
        db.where('lc.cluster_nid', nid)
    db.get()

    h = {}
    for row in db.result():
        h.setdefault(row[1], {'name': row[1], 'description': row[2],
                              'source': set()})
        h[row[1]]['source'].add(row[0])
    data = []
    for k, v in h.iteritems():
        v['source'] = list(v['source'])
        data.append(v)
    return data


def get_cluster_loopsource_size(db, cluster, nid=None):
    if cluster.lower() == 'class':
        query = 'cs.class_nid'
    if cluster.lower() == 'subclass':
        query = 'cs.nid'

    db.select(query)
    db.table('cluster_subclass cs')
    db.join('loop2cluster l2c', 'l2c.cluster_nid = cs.nid')
    db.join('loop2chain lc', 'lc.loop_id = l2c.loop_nid')
    db.where('lc.assignation', 'D')
    if nid is not None:
        if isinstance(nid, int):
            db.where('nid', nid)
        elif isinstance(nid, list):
            db.where_in('nid', nid)
    db.group_by('{0}, lc.chain'.format(query))
    db.get()
    return Counter([x[0] for x in db.result()])


def get_cluster_extended_loop_species_size(db, cluster, taxID, nid=None):
    if cluster.lower() == 'class':
        query = 'cs.class_nid'
    if cluster.lower() == 'subclass':
        query = 'cs.nid'

    db.select(query)
    db.table('cluster_subclass cs')
    db.join('loop2cluster l2c', 'l2c.cluster_nid = cs.nid')
    db.join('loop2chain lc', 'lc.loop_id = l2c.loop_nid')
    db.join('chain2uniprot c2u',
            'c2u.chain = lc.chain AND lc.start >= c2u.start ' +
            'AND lc.end <= c2u.end')
    db.join('uniprot2taxid u2t',
            'u2t.uniprot = c2u.uniprot AND u2t.taxid = {0}'.format(taxID))
    db.where_in('lc.assignation', ['D', 'H'])
    if nid is not None:
        if isinstance(nid, int):
            db.where('nid', nid)
        elif isinstance(nid, list):
            db.where_in('nid', nid)
    db.group_by('{0}, lc.loop_id'.format(query))
    db.get()
    return Counter([x[0] for x in db.result()])
