import sys
from . import cluster as klstr

''' The GET INSTANCES functions are assumed to be performed in order to build the enrichment tables'''
def get_all_instances_of(db, external, mode):
    ClusterInstances.external = external.lower()
    ClusterInstances.mode     = mode.lower()
    ClusterInstances.debug    = db._dbug

    if db._dbug: sys.stderr.write('Creating SQL query for {0.external} / {0.mode}\n'.format(ClusterInstances))

    db.select('c2u.chain')
    if external.lower() == 'enzyme': 
        db.select('ext.enzyme')
        db.table('uniprot2enzyme ext')
        db.join('chain2uniprot c2u','c2u.uniprot = ext.uniprot')
    elif external.lower() == 'go':
        db.select('ext.GO')
        db.table('uniprot2GO ext')
        db.join('chain2uniprot c2u','c2u.uniprot = ext.uniprot')
    elif external.lower() == 'drugbank':
        db.select('ext.drugbank_id')
        db.table('drugBank_target ext')
        db.join('chain2uniprot c2u','c2u.uniprot = ext.uniprot')
    elif external.lower() == 'scop':
        db.select('ext.family, ext.superfamily, ext.fold, ext.class')
        db.table('scop ext')
        db.join('chain2scop c2u','c2u.domain = ext.domain')
    db.join('loop2chain l2c','l2c.chain=c2u.chain AND l2c.assignation="D" AND l2c.start>=c2u.start AND l2c.end<=c2u.end')
    db.join('loop_description ld','ld.nid=l2c.loop_id')
    db.get()

    cluster = ClusterInstances(0, 0)
    if db._dbug: sys.stderr.write('\tAdding Results.\n')
    for row in db.result():
        cluster.add_instance(row[::-1])
    result = []
    if db._dbug: sys.stderr.write('\tProcessing.\n')
    for line in repr(cluster).split('\n'):
        line = line.split('\t')
        result.append(tuple([line[1], int(line[2])]))
    return result

def get_instances_for(db, cluster, external, mode):
    ClusterInstances.external = external.lower()
    ClusterInstances.mode     = mode.lower()

    if   mode == 'regular':     cluster_sizes = klstr.get_cluster_size(db, cluster)
    elif mode == 'compressed':  cluster_sizes = klstr.get_cluster_loopsource_size(db, cluster)

    if   cluster == 'class':    search_param = 'class_nid'
    elif cluster == 'subclass': search_param = 'nid'
    
    if db._dbug: sys.stderr.write('Creating SQL query for {0.external} / {0.mode}\n'.format(ClusterInstances))

    db.select('cs.{0}, lc.chain'.format(search_param))
    if external.lower() == 'enzyme':        db.select('ext.enzyme')
    elif external.lower() == 'go':          db.select('ext.GO')
    elif external.lower() == 'drugbank':    db.select('ext.drugbank_id')
    elif external.lower() == 'scop':        db.select('ext.family, ext.superfamily, ext.fold, ext.class')
    db.table('cluster_subclass  cs')
    db.join('loop2cluster       l2c',   'l2c.cluster_nid = cs.nid'          )
    db.join('loop2chain         lc',    'lc.loop_id      = l2c.loop_nid'    )
    if external.lower() == 'enzyme':
        db.join('chain2uniprot c2u',    'c2u.chain=lc.chain AND lc.start>=c2u.start AND lc.end<=c2u.end')
        db.join('uniprot2enzyme ext',   'ext.uniprot = c2u.uniprot'         )
    elif external.lower() == 'go':
        db.join('chain2uniprot c2u',    'c2u.chain=lc.chain AND lc.start>=c2u.start AND lc.end<=c2u.end')
        db.join('uniprot2GO ext',       'ext.uniprot = c2u.uniprot'         )
    elif external.lower() == 'drugbank':
        db.join('chain2uniprot c2u',    'c2u.chain=lc.chain AND lc.start>=c2u.start AND lc.end<=c2u.end')
        db.join('drugBank_target ext',  'ext.uniprot = c2u.uniprot'         )
    elif external.lower() == 'scop':    
        db.join('chain2scop c2u',       'c2u.chain=lc.chain AND lc.start>=c2u.start AND lc.end<=c2u.end')
        db.join('scop ext',             'c2u.domain = ext.domain'           )                  
    db.where('lc.assignation',          'D'                                 )
    db.get()
    dataDIC = {}
    if db._dbug: sys.stderr.write('\tAdding Results.\n')
    for row in db.result():
        dataDIC.setdefault(row[0], ClusterInstances(row[0], cluster_sizes[row[0]]))
        data = tuple(row[1:])
        dataDIC[row[0]].add_instance(tuple(data[::-1]))

    result = {}
    if db._dbug: sys.stderr.write('\tProcessing.\n')
    for key in dataDIC:
        for line in repr(dataDIC[key]).split('\n'):
            line = line.split('\t')
            result.setdefault(line[0],{'n':line[3],'k':[]})
            result[line[0]]['k'].append(tuple([line[1],int(line[2])]))
    return result

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
            self.list_instances.append(tuple([instance[0],instance[-1]]))
            self.list_instances.append(tuple([instance[1],instance[-1]]))
            self.list_instances.append(tuple([instance[2],instance[-1]]))
            self.list_instances.append(tuple([instance[3],instance[-1]]))

    def _process_enzyme(self):
        new_instances         = set()
        
        def enzyme_parent(ec):
            ec = ec.split('.')
            if ec[1]   == '-': return None
            if ec[2]   == '-': ec[1] = '-'
            elif ec[3] == '-': ec[2] = '-'
            else:              ec[3] = '-'
            return '.'.join(ec)

        i = 0
        for instance in self.list_instances:
            ext, prt = instance[0], instance[1]
            if self.debug: 
                if i%100 == 0:
                    sys.stderr.write('\tProcessed Rows: {0}\n'.format(i))
                i += 1
            while(True):
                ext = enzyme_parent(ext)
                if ext is None: break
                if (ext, prt) not in new_instances:
                    new_instances.add((ext, prt))
        self.list_instances.extend(list(new_instances))

    def __repr__(self):
        if self.debug: sys.stderr.write('Total rows: {0}\n'.format(len(self.list_instances)))
        if self.external == 'enzyme': self._process_enzyme()
        [self.dict_instances.setdefault(x[0],[]).append(x[1]) for x in self.list_instances]
        text = []
        for instance in self.dict_instances:
            if self.mode   == 'regular':    population = self.dict_instances[instance]
            elif self.mode == 'compressed': population = list(set(self.dict_instances[instance]))
            population_success = len(population)

            text.append('{0.cluster_nid}\t{1}\t{2}\t{0.cluster_size}\t{3}'.format(self, instance, population_success, repr(population)))
        return "\n".join(text)
