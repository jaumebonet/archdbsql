import sys
from . import cluster as klstr


def tofloat(value):
    try:
        return float(value)
    except:
        return 1


def get_enrichment_representative(db, cluster_nid, external, external_id, assignation='D'):
    db.select('ld.loop_id, ld.sequence, ld.ss')
    db.table('loop_description ld')
    db.join('loop2cluster lc', 'lc.loop_nid=ld.nid')
    db.join('loop2chain l2c', 'l2c.loop_id=ld.nid AND l2c.assignation="{0}"'.format(assignation))
    db.join('chain2uniprot c2u', 'c2u.chain=l2c.chain ' +
            'AND l2c.start>=c2u.start AND l2c.end<=c2u.end')
    if external.lower() == 'enzyme':
        db.join('uniprot2enzyme u2e', 'u2e.uniprot=c2u.uniprot')
        db.like('u2e.enzyme', external_id.split('.')[0], 'after')
    elif external.lower() == 'go':
        db.join('uniprot2GO u2g', 'u2g.uniprot=c2u.uniprot')
        db.join('GO g', 'u2g.GO=g.nid')
        db.where('g.id', external_id)
    elif external.lower() == 'drugbank':
        pass  # TODO
    elif external.lower() == 'scop':
        pass  # TODO
    db.where('lc.cluster_nid', cluster_nid)
    db.order_by('lc.clust_order')
    db.limit(1)
    db.get()

    data = {'loop': '', 'sequence': '', 'structure': ''}
    for row in db.result():
        data['loop']      = row[0]
        data['sequence']  = row[1]
        data['structure'] = row[2]

    if assignation == 'D' and len(data['sequence']) == 0:
        return get_enrichment_representative(db, cluster_nid, external, external_id, 'H')
    else:
        return data


def get_enrichment(db, cluster, external, nid):
    s = ','.join(['enrichment_pvalue', 'frequency', 'logodd',
                  'mutual_information', 'number_instances'])
    if external.lower() == 'enzyme':
        db.select('e.id, e.description, e.level, {0}'.format(s))
        db.table('enzyme_{0}_enrichment r'.format(cluster))
        db.join('enzyme e', 'e.id=r.enzyme_id')
    elif external.lower() == 'go':
        db.select('g.id, g.name, g.namespace, {0}'.format(s))
        db.table('go_{0}_enrichment r'.format(cluster))
        db.join('GO g', 'g.nid=r.GO_nid')
    elif external.lower() == 'drugbank':
        ## TODO
        db.table('drugBank_{0}_enrichment'.format(cluster))
        ## TODO
    elif external.lower() == 'scop':
        ## TODO
        db.table('scop_{0}_enrichment'.format(cluster))
        ## TODO
    if isinstance(nid, list):
        db.where_in('r.{0}_nid'.format(cluster), nid)
    else:
        db.where('r.{0}_nid'.format(cluster), nid)
    db.get()

    r = {}
    for row in db.result():
        r.setdefault(row[0], {'id': row[0], 'name': row[1], 'info': row[2],
                              'pvalue': tofloat(row[3]), 'MI': tofloat(row[6]),
                              'logodd': tofloat(row[5]), 'k':  int(row[7]),
                              'frequency': tofloat(row[4])})
        if tofloat(row[3]) < r[row[0]]['pvalue']:
            r[row[0]]['pvalue'] = tofloat(row[3])
        if tofloat(row[6]) > r[row[0]]['MI']:
            r[row[0]]['MI'] = tofloat(row[6])
        if tofloat(row[4]) > r[row[0]]['frequency']:
            r[row[0]]['frequency'] = tofloat(row[4])
        if int(row[7]) > r[row[0]]['k']:
            r[row[0]]['k'] = int(row[7])
        if tofloat(row[5]) > r[row[0]]['logodd']:
            r[row[0]]['logodd'] = tofloat(row[5])

    if external.lower() == 'enzyme':
        for k, v in r.iteritems():
            if v['info'] == 2:
                parent = enzyme_parent(k)
                if parent in r:
                    v['name'] = r[parent]['name'] + ' ' + v['name']
        for k, v in r.iteritems():
            if v['info'] == 3:
                parent = enzyme_parent(k)
                if parent in r:
                    v['name'] = r[parent]['name'] + ' ' + v['name']
        for k, v in r.iteritems():
            if v['info'] == 4:
                parent = enzyme_parent(k)
                if parent in r:
                    v['name'] = r[parent]['name'] + ' ' + v['name']
    data = []
    for k, v in r.iteritems():
        data.append(v)
    return data

''' The GET INSTANCES functions are assumed to be performed in order to build the enrichment tables'''


def get_all_instances_of(db, external, mode):
    ClusterInstances.external = external.lower()
    mode                      = mode.split('.')
    if len(mode) == 1:
        taxID = None
    else:
        taxID = int(mode[1])
    mode                      = mode[0]
    ClusterInstances.mode     = mode.lower()
    ClusterInstances.debug    = db._dbug

    if db._dbug:
        sys.stderr.write('Creating SQL query for {0.external} / {0.mode}\n'.format(ClusterInstances))

    db.select('c2u.chain')
    if external.lower() == 'enzyme':
        unit = 'ext.enzyme'
        db.select('ext.enzyme')
        db.table('uniprot2enzyme ext')
        db.join('chain2uniprot c2u', 'c2u.uniprot = ext.uniprot')
    elif external.lower() == 'go':
        unit = 'ext.GO'
        db.select('ext.GO')
        db.table('uniprot2GO ext')
        db.join('chain2uniprot c2u', 'c2u.uniprot = ext.uniprot')
    elif external.lower() == 'drugbank':
        unit = 'ext.drugbank_id'
        db.select('ext.drugbank_id')
        db.table('drugBank_target ext')
        db.join('chain2uniprot c2u', 'c2u.uniprot = ext.uniprot')
    elif external.lower() == 'scop':
        unit = 'ext.family'
        db.select('ext.family, ext.superfamily, ext.fold, ext.class')
        db.table('scop ext')
        db.join('chain2scop c2u', 'c2u.domain = ext.domain')
    if taxID is not None:
        if external.lower() in ['enzyme', 'go', 'drugbank']:
            if taxID != 0:
                db.join('uniprot2taxid u2t', 'u2t.uniprot = c2u.uniprot AND u2t.taxid = {0}'.format(taxID))
            db.join('loop2chain l2c', 'l2c.chain=c2u.chain AND (l2c.assignation="D" OR l2c.assignation="H") AND l2c.start>=c2u.start AND l2c.end<=c2u.end')
        elif external.lower() == 'scop':
            db.join('chain2uniprot n2u', 'n2u.chain = c2u.chain')
            if taxID != 0:
                db.join('uniprot2taxid u2t', 'u2t.uniprot = n2u.uniprot AND u2t.taxid = {0}'.format(taxID))
            db.join('loop2chain l2c', 'l2c.chain=c2u.chain AND (l2c.assignation="D" OR l2c.assignation="H") AND l2c.start>=c2u.start AND l2c.end<=c2u.end')
    else:
        db.join('loop2chain l2c', 'l2c.chain=c2u.chain AND l2c.assignation="D" AND l2c.start>=c2u.start AND l2c.end<=c2u.end')
    db.join('loop_description ld', 'ld.nid=l2c.loop_id')
    if taxID is not None:
        if taxID != 0:
            db.group_by('ld.nid, u2t.uniprot, {0}'.format(unit))
        else:
            db.group_by('ld.nid, {0}'.format(unit))
    db.get()

    cluster = ClusterInstances(0, 0)
    if db._dbug:
        sys.stderr.write('\tAdding Results.\n')
    for row in db.result():
        cluster.add_instance(row[::-1])
    result = []
    if db._dbug:
        sys.stderr.write('\tProcessing.\n')
    for line in repr(cluster).split('\n'):
        line = line.split('\t')
        result.append(tuple([line[1], int(line[2])]))
    return result


def get_instances_for(db, cluster, external, mode):
    ClusterInstances.external = external.lower()
    mode                      = mode.split('.')
    if len(mode) == 1:
        taxID = None
    else:
        taxID = int(mode[1])
    mode                      = mode[0]
    ClusterInstances.mode     = mode.lower()

    if mode == 'regular':
        if taxID is None or taxID == 0:
            cluster_sizes = klstr.get_cluster_size(db, cluster)
        else:
            cluster_sizes = klstr.get_cluster_extended_loop_species_size(db, cluster, taxID)
    elif mode == 'compressed':
        cluster_sizes = klstr.get_cluster_loopsource_size(db, cluster)

    if cluster == 'class':
        search_param = 'class_nid'
    elif cluster == 'subclass':
        search_param = 'nid'

    if db._dbug:
        sys.stderr.write('Creating SQL query for {0.external} / {0.mode}\n'.format(ClusterInstances))

    db.select('cs.{0}, lc.chain'.format(search_param))
    if external.lower() == 'enzyme':
        db.select('ext.enzyme')
    elif external.lower() == 'go':
        db.select('ext.GO')
    elif external.lower() == 'drugbank':
        db.select('ext.drugbank_id')
    elif external.lower() == 'scop':
        db.select('ext.family, ext.superfamily, ext.fold, ext.class')
    db.table('cluster_subclass  cs')
    db.join('loop2cluster       l2c',   'l2c.cluster_nid = cs.nid')
    db.join('loop2chain         lc',    'lc.loop_id      = l2c.loop_nid')
    if external.lower()   == 'enzyme':
        unit = 'ext.enzyme'
        db.join('chain2uniprot c2u',    'c2u.chain=lc.chain AND lc.start>=c2u.start AND lc.end<=c2u.end')
        db.join('uniprot2enzyme ext',   'ext.uniprot = c2u.uniprot')
    elif external.lower() == 'go':
        unit = 'ext.GO'
        db.join('chain2uniprot c2u',    'c2u.chain=lc.chain AND lc.start>=c2u.start AND lc.end<=c2u.end')
        db.join('uniprot2GO ext',       'ext.uniprot = c2u.uniprot')
    elif external.lower() == 'drugbank':
        unit = 'ext.drugbank_id'
        db.join('chain2uniprot c2u',    'c2u.chain=lc.chain AND lc.start>=c2u.start AND lc.end<=c2u.end')
        db.join('drugBank_target ext',  'ext.uniprot = c2u.uniprot')
    elif external.lower() == 'scop':
        unit = 'ext.family'
        db.join('chain2scop c2u',       'c2u.chain=lc.chain AND lc.start>=c2u.start AND lc.end<=c2u.end')
        db.join('scop ext',             'c2u.domain = ext.domain')
    if taxID is not None:
        if external.lower() in ['enzyme', 'go', 'drugbank'] and taxID != 0:
            db.join('uniprot2taxid u2t', 'u2t.uniprot = c2u.uniprot AND u2t.taxid = {0}'.format(taxID))
        elif external.lower() == 'scop':
            db.join('chain2uniprot n2u', 'n2u.chain   = lc.chain AND lc.start>=n2u.start AND lc.end<=n2u.end')
            if taxID != 0:
                db.join('uniprot2taxid u2t', 'u2t.uniprot = n2u.uniprot AND u2t.taxid = {0}'.format(taxID))
        db.where_in('lc.assignation',   ['D', 'H'])
    else:
        db.where('lc.assignation',      'D')
    if taxID is not None:
        db.group_by('cs.{0}, lc.loop_id, {1}'.format(search_param, unit))
    db.get()
    dataDIC = {}
    if db._dbug:
        sys.stderr.write('\tAdding Results.\n')
    for row in db.result():
        dataDIC.setdefault(row[0], ClusterInstances(row[0], cluster_sizes[row[0]]))
        data = tuple(row[1:])
        dataDIC[row[0]].add_instance(tuple(data[::-1]))

    result = {}
    if db._dbug:
        sys.stderr.write('\tProcessing.\n')
    for key in dataDIC:
        for line in repr(dataDIC[key]).split('\n'):
            line = line.split('\t')
            result.setdefault(line[0], {'n': line[3], 'k': []})
            result[line[0]]['k'].append(tuple([line[1], int(line[2])]))
    return result


def enzyme_parent(ec):
    ec = ec.split('.')
    if ec[1]   == '-':
        return None
    if ec[2]   == '-':
        ec[1] = '-'
    elif ec[3] == '-':
        ec[2] = '-'
    else:
        ec[3] = '-'
    return '.'.join(ec)


class ClusterInstances(object):
    external = ''
    mode     = ''
    debug    = False

    def __init__(self, cluster_nid, size):
        self.cluster_nid    = cluster_nid
        self.cluster_size   = size
        self.list_instances = []
        self.dict_instances = {}

    def add_instance(self, instance):
        if self.external != 'scop':
            self.list_instances.append(instance)
        else:
            self.list_instances.append(tuple([instance[0], instance[-1]]))
            self.list_instances.append(tuple([instance[1], instance[-1]]))
            self.list_instances.append(tuple([instance[2], instance[-1]]))
            self.list_instances.append(tuple([instance[3], instance[-1]]))

    def process(self):
        if self.external == 'enzyme':
            self._process_enzyme()

    def _process_enzyme(self):
        new_instances         = set()

        i = 0
        for instance in self.list_instances:
            ext, prt = instance[0], instance[1]
            if self.debug:
                if i % 100 == 0:
                    sys.stderr.write('\tProcessed Rows: {0}\n'.format(i))
                i += 1
            while(True):
                ext = enzyme_parent(ext)
                if ext is None:
                    break
                if (ext, prt) not in new_instances:
                    new_instances.add((ext, prt))
        self.list_instances.extend(list(new_instances))

    def __repr__(self):
        if self.debug:
            sys.stderr.write('Total rows: {0}\n'.format(len(self.list_instances)))
        if self.external == 'enzyme':
            self._process_enzyme()
        [self.dict_instances.setdefault(x[0], []).append(x[1]) for x in self.list_instances]
        text = []
        for instance in self.dict_instances:
            if self.mode   == 'regular':
                population = self.dict_instances[instance]
            elif self.mode == 'compressed':
                population = list(set(self.dict_instances[instance]))
            population_success = len(population)

            text.append('{0.cluster_nid}\t{1}\t{2}\t{0.cluster_size}\t{3}'.format(self, instance, population_success, repr(population)))
        return "\n".join(text)
