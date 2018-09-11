def get_method_nid(db, method_name):
    db.select('nid')
    db.table('method')
    db.where('name', method_name)
    db.get()
    nid = db.row()
    return int(nid[0]) if nid is not None else nid


def get_method_name(db, method_nid):
    db.select('name')
    db.table('method')
    db.where('nid', method_nid)
    db.get()
    nid = db.row()
    return nid[0] if nid is not None else nid

