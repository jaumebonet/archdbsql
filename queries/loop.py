def count_loops(db):
    db.select('COUNT(*)')
    db.table('loop_description')
    db.get()
    count = db.row()
    return int(count[0]) if count is not None else count

def count_loops_source_pdb(db):
    db.select('COUNT(*)')
    db.table('loop2chain')
    db.where('assignation','D')
    db.group_by('chain')
    db.get()
    return len(db.result())

def count_loops_assigned_to_species(db, taxID):
    db.select('COUNT(*)')
    db.table('loop2chain lc')
    db.join('chain2uniprot c2u',  'c2u.chain   = lc.chain')
    if   isinstance(taxID, int):  selector     = "u2t.taxid = {0}".format(taxID)
    elif isinstance(taxID, list): selector     = "u2t.taxid IN {0}".format(taxID)
    db.join('uniprot2taxid u2t',  'u2t.uniprot = c2u.uniprot AND {0}'.format(selector))
    db.where('lc.assignation',    'D')
    db.or_where('lc.assignation', 'H')
    db.group_by('loop_id')
    db.get()
    count = db.result()
    return len(count) if count is not None else 0
