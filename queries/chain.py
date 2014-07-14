def list_chains_with_loop_assigned(db, assignation = 'D'):
    db.select('CONCAT(c.pdb, "_" ,c.chain)')
    db.table('chain c')
    db.join('loop2chain lc', 'lc.chain=c.nid')
    if isinstance(assignation, list):
        db.where_in('lc.assignation', assignation)
    else:
        db.where('lc.assignation', assignation)
    db.group_by('c.nid')
    db.get()
    return [x[0] for x in db.result()]
