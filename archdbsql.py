from .dbconnector.database  import Database
from .queries               import method
from .queries               import loop
from .queries               import enrichment
from .queries               import taxid

class ArchDBsql(object):

    external_relations = ['enzyme', 'GO', 'drugBank', 'SCOP']
    cluster_types      = ['class',  'subclass']

    def __init__(self, dbhost = None, dbuser = None, dbpass = None, dbname = None, dbug = False):
        self._db  = Database(dbhost = dbhost, dbuser = dbuser, dbpass = dbpass, dbname = dbname, dbug = dbug)

    @property 
    def debug(self): return self._db._dbug
    @debug.setter
    def debug(self, value): self._db._dbug = value

    # METHOD RELATED FUNCTIONS
    def get_method_nid(self, method_name):
        return method.get_method_from_nid(self._db, method_name)

    # LOOP RELATED FUNCTIONS
    def get_loop_count(self):
        return loop.count_loops(self._db)
    def get_loop_source_chain_count(self):
        return loop.count_loops_source_pdb(self._db)
    def get_loop_specie_count(self, taxID):
        return loop.count_loops_assigned_to_species(self._db, taxID)

    # ENRICHMENT ANALYSIS RELATED FUNTIONS
    def get_all_instances_of(self, external, mode):
        self._check_external_relations(external)
        return enrichment.get_all_instances_of(self._db, external, mode)
    def get_instances_for(self, cluster, external, mode):
        self._check_external_relations(external)
        self._check_cluster_types(cluster)
        return enrichment.get_instances_for(self._db, cluster, external, mode)

    # TAXID RELATED FUNCTIONS
    def exists_taxid(self, taxID):
        return True if taxid.get_tax_from_id(self._db, taxID) is not None else False

    def exists_specie(self, taxID):
        data = taxid.get_tax_from_id(self._db, taxID)
        if data is None or data[3] != 'species': return False
        return True

    #CHECKERS
    def _check_external_relations(self, relation):
        if not relation in self.external_relations: 
            raise AttributeError('External relation options are {0}\n'.format(repr(self.external_relations)))
    def _check_cluster_types(self, ctype):
        if not ctype in self.cluster_types:
            raise AttributeError('Cluster options are {0}\n'.format(repr(self.cluster_types)))
