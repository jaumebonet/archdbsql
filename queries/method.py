def get_method_nid(db, method_name):
    db.select('nid')
    db.table('method')
    db.where('name', method_name)
    db.get()
    nid = db.row()
    return int(nid[0]) if nid is not None else nid