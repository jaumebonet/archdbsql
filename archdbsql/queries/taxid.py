def get_tax_from_id(db, taxid):
    db.table('taxid')
    db.where('id', taxid)
    db.get()
    return db.row()

def get_specie_pdbchain(db, taxid):
    db.select('c2u.chain')
    db.table('taxid t')
    db.join('uniprot2taxid u2t',    'u2t.taxid = t.id AND u2t.relationship = "is"')
    db.join('chain2uniprot c2u',    'c2u.uniprot = u2t.uniprot')
    if   isinstance(taxid, int):    db.where('t.id',    taxid)
    elif isinstance(taxid, list):   db.where_in('t.id', taxid)
    db.get()
    return set([x[0] for x in db.result()])