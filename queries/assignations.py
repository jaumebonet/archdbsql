from .enrichment import ClusterInstances
from collections import Counter
import sys


def uniprot2(db, external):
    db.select('u2.uniprot')
    if external.lower() == 'enzyme':
        ClusterInstances.external = external.lower()
        db.select('u2.enzyme')
        db.table('uniprot2enzyme u2')
    if external.lower().startswith('go'):
        if ':' in external:
            namespace = external.split(':')[1]
        db.table('uniprot2GO u2')
        db.select('g.id, g.namespace')
        if ':' in external:
            db.join('GO g', 'g.nid=u2.GO AND g.namespace="{0}"'.format(namespace))
        else:
            db.join('GO g', 'g.nid=u2.GO')
    db.get()

    data = []
    for row in db.result():
        if db._dbug:
            sys.stderr.write("[Q] " + "\t".join([str(x) for x in row]) + "\n")
        if external.lower() == 'enzyme':
            c = ClusterInstances(0, 0)
            c.add_instance([row[1], 'func'])
            c.process()
            for i in c.list_instances:
                level = 4 - Counter(i[0])['-']
                data.append([row[0], i[0], level])
        else:
            data.append(list(row))

    return data
