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
