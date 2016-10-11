from .dbconnector.database  import Database
from .queries               import method
from .queries               import cluster
from .queries               import loop
from .queries               import chain
from .queries               import enrichment
from .queries               import taxid
from .queries               import assignations


class ArchDBsql(object):

    external_relations = ['enzyme', 'GO', 'drugBank', 'SCOP', 'GO:M']
    cluster_types      = ['class', 'subclass']
    contact_types      = ['sites', 'ligands']

    def __init__(self, dbhost=None, dbuser=None,
                 dbpass=None, dbname=None, dbug=False):

        if dbpass == 'None':
            dbpass = None
        self._db  = Database(dbhost=dbhost, dbuser=dbuser,
                             dbpass=dbpass, dbname=dbname, dbug=dbug)

        self._methods = {}

    @property
    def debug(self):
        return self._db._dbug

    @debug.setter
    def debug(self, value):
        self._db._dbug = value

    # METHOD RELATED FUNCTIONS
    def get_method_nid(self, method_name):
        if method_name not in self._methods:
            m = method.get_method_nid(self._db, method_name)
            self._methods[method_name] = m
        return self._methods[method_name]

    # CLUSTER RELATED FUNCTIONS
    def list_subclasses(self):
        return cluster.list_all(self._db)

    def get_subclass_size(self, subclass_nid ):
        return cluster.get_cluster_size( self._db, "subclass", subclass_nid )

    def get_subclass_from_geometries(
            self, methodID, ctrtype, length, distance, theta, rho, delta):
        return cluster.get_subclass_from_geometries_range(
            self._db, methodID, ctrtype, length, distance, theta, rho, delta)

    def get_subclass_from_geometries_range(
            self, methodID, ctrtype, length, distance, theta, rho, delta,
            length_range, dist_range, theta_range, rho_range, delta_range):
        return cluster.get_subclass_from_geometries_range(
            self._db, methodID, ctrtype, length, distance, theta, rho, delta,
            length_range, dist_range, theta_range, rho_range, delta_range)

    def get_subclass_contacts(self, subclass_nid, contact_types):
        self._check_contac_types(contact_types)
        return cluster.get_subclass_contacts(self._db, subclass_nid,
                                             contact_types)

    def get_similar_subclasses_to(self, subclass_nid, method_name,
                                  length_range, dist_range, theta_range,
                                  rho_range, delta_range):
        methodID = self.get_method_nid(method_name)
        return cluster.get_similar_subclasses_to(self._db, subclass_nid, methodID,
                                                 length_range, dist_range, theta_range,
                                                 rho_range, delta_range)

    def get_subclass_loops(self, subclass_nid):
        return cluster.get_subclass_loops(self._db, subclass_nid)

    def get_subclass_representative(self, subclass_nid):
        return cluster.get_subclass_representative(self._db, subclass_nid)

    # LOOP RELATED FUNCTIONS
    def get_loop_count(self):
        return loop.count_loops(self._db)

    def get_loop_source_chain_count(self):
        return loop.count_loops_source_pdb(self._db)

    def get_loop_specie_count(self, taxID):
        return loop.count_loops_assigned_to_species(self._db, taxID)

    def get_loops_for_protein(self, proteinID, method_name = None):
        if method_name is None:
            methodID = None
        else:
            methodID = self.get_method_nid(method_name)
        return loop.get_loops_for_protein(self._db, proteinID, methodID)

    def get_info_loop(self, query, loop_id):
        return loop.get_info_loop(self._db, query, loop_id)

    # CHAIN FUNCTIONS
    def list_source_chains(self):
        return chain.list_chains_with_loop_assigned(self._db)

    def list_source_and_identical_chains(self):
        return chain.list_chains_with_loop_assigned(self._db, ['D', 'I'])

    # ENRICHMENT FUNCTIONS
    def get_enrichment(self, cluster, external, nid):
        self._check_external_relations(external)
        self._check_cluster_types(cluster)
        return enrichment.get_enrichment(self._db, cluster, external, nid)

    def get_enrichment_representative(self, cluster_nid,
                                      external, external_id):
        return enrichment.get_enrichment_representative(self._db, cluster_nid,
                                                        external, external_id)

    # ENRICHMENT ANALYSIS RELATED FUNCTIONS
    def get_all_instances_of(self, external, mode):
        self._check_external_relations(external)
        return enrichment.get_all_instances_of(self._db, external, mode)

    def get_instances_for(self, cluster, external, mode):
        self._check_external_relations(external)
        self._check_cluster_types(cluster)
        return enrichment.get_instances_for(self._db, cluster, external, mode)

    # TAXID RELATED FUNCTIONS
    def exists_taxid(self, taxID):
        if taxid.get_tax_from_id(self._db, taxID) is not None:
            return True
        return False

    def exists_specie(self, taxID):
        data = taxid.get_tax_from_id(self._db, taxID)
        if data is None or data[3] != 'species':
            return False
        return True

    # GENERAL RELATIONS
    def get_uniprot_assignations(self, external):
        self._check_external_relations(external)
        return assignations.uniprot2(self._db, external)

    # CHECKERS
    def _check_external_relations(self, relation):
        msg = 'External relation options are {0}\n'
        msg = msg.format(repr(self.external_relations))
        if relation not in self.external_relations:
            raise AttributeError(msg)

    def _check_cluster_types(self, ctype):
        msg = 'Cluster options are {0}\n'
        msg = msg.format(repr(self.cluster_types))
        if ctype not in self.cluster_types:
            raise AttributeError(msg)

    def _check_contac_types(self, ctype):
        msg = 'Contact options are {0}\n'
        msg = msg.format(repr(self.contact_types))
        if ctype not in self.contact_types:
            raise AttributeError(msg)
