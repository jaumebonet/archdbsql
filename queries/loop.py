def count_loops(db):
    db.select('COUNT(*)')
    db.table('loop_description')
    db.get()
    count = db.row()
    return int(count[0]) if count is not None else count


def count_loops_source_pdb(db):
    db.select('COUNT(*)')
    db.table('loop2chain')
    db.where('assignation', 'D')
    db.group_by('chain')
    db.get()
    return len(db.result())


def count_loops_assigned_to_species(db, taxID):
    db.select('COUNT(*)')
    db.table('loop2chain lc')
    db.join('chain2uniprot c2u',  'c2u.chain   = lc.chain')
    if taxID != 0:
        if isinstance(taxID, int):
            selector = "u2t.taxid = {0}".format(taxID)
        elif isinstance(taxID, list):
            selector = "u2t.taxid IN {0}".format(taxID)
        db.join('uniprot2taxid u2t',  'u2t.uniprot = c2u.uniprot AND {0}'.format(selector))
    db.where('lc.assignation',    'D')
    db.or_where('lc.assignation', 'H')
    db.group_by('loop_id')
    db.get()
    count = db.result()
    return len(count) if count is not None else 0


def get_loops_for_protein(db, proteinID, methodID):
    p = proteinID.upper().split('_')
    db.select('ld.loop_id, ld.ss1L, ld.ss2L, ld.length')
    db.select('CONCAT(l2c.start, l2c.idxs), CONCAT(l2c.end, l2c.idxe)')
    db.table('loop_description ld')
    db.join('loop2chain l2c', 'l2c.loop_id=ld.nid')
    db.join('chain c', 'l2c.chain=c.nid')
    if methodID is not None:
        # db.select("GROUP_CONCAT(CONCAT(m.name, '.', cs.name))")
        db.select('GROUP_CONCAT(cs.name), GROUP_CONCAT(cs.nid)')
        db.join('loop2cluster lc', 'lc.loop_nid=ld.nid', 'left')
        db.join('cluster_subclass cs', 'cs.nid=lc.cluster_nid', 'left')
        db.join('cluster_class cc', 'cc.nid=cs.class_nid', 'left')
        # db.join('method m', 'm.nid=cc.method', 'left')
        db.where('cc.method', methodID)
        db.group_by('ld.loop_id')
    db.where('c.pdb', p[0])
    db.where('c.chain', p[1])

    db.get()
    return db.result()


def get_info_loop(db, query, loop_id):
    db.select(query)
    db.table('loop_description')
    db.where('loop_id', loop_id)
    db.get()
    return db.row()
