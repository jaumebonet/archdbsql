import mysql.connector as MySQLdb
import time
import traceback
import re
# import locale
import math

__all__ = ['Database']


class Database:
    """a python mysql query builder in codeIgniter activerecord style"""

    def __init__(self, dbhost=None, dbuser=None, dbpass=None,
                 dbname=None, dbug=False):
        """Initialize the database connection"""
        self._dbhost = dbhost
        self._dbuser = dbuser
        self._dbpass = dbpass
        self._dbname = dbname
        self._dbug   = dbug

        # from DB_active_rec.php
        self.ar_select              = []
        self.ar_distinct            = False
        self.ar_from                = []
        self.ar_join                = []
        self.ar_where               = []
        self.ar_like                = []
        self.ar_groupby             = []
        self.ar_having              = []
        self.ar_keys                = []
        self.ar_limit               = False
        self.ar_offset              = False
        self.ar_order               = False
        self.ar_orderby             = []
        self.ar_set                 = {}    # changed from [] to {}
        self.ar_wherein             = []
        self.ar_aliased_tables      = []
        self.ar_store_array         = []

        # Active Record Caching variables
        self.ar_caching             = False
        self.ar_cache_exists        = []
        self.ar_cache_select        = []
        self.ar_cache_from          = []
        self.ar_cache_join          = []
        self.ar_cache_where         = []
        self.ar_cache_like          = []
        self.ar_cache_groupby       = []
        self.ar_cache_having        = []
        self.ar_cache_orderby       = []
        self.ar_cache_set           = {}    # changed from [] to {}

        self.ar_no_escape           = []
        self.ar_cache_no_escape     = []

        # from DB_driver.php
        self.dbprefix       = ''
        self.swap_pre       = ''
        self.benchmark      = 0
        self.query_count    = 0
        self.bind_marker    = '?'
        self.save_queries   = True
        self.queries        = []
        self.query_times    = []
        self.data_cache     = []
        self.trans_enabled  = True
        self.trans_strict   = True
        self._trans_depth   = 0
        self._trans_status  = True  # Used with transactions to determine if a rollback should occur
        self.cache_on       = False
        self.cachedir       = ''
        self.cache_autodel  = False
        self.CACHE          = None  # The cache class object

        # Private variables
        self.__protect_identifiers   = True
        self._reserved_identifiers  = ['*']  # Identifiers that should NOT be escaped

        # from mysql_driver.php
        self.dbdriver = 'mysql'
        # The character used for escaping
        self._escape_char = '`'

        # clause and character used for LIKE escape sequences - not used in MySQL
        self._like_escape_str = ''
        self._like_escape_chr = ''

        # Whether to use the MySQL "delete hack" which allows the number
        # of affected rows to be shown. Uses a preg_replace when enabled,
        # adding a bit more processing to all queries.
        self.delete_hack = True

        # The syntax to count rows is slightly different across different
        # database engines, so this string appears in each driver and is
        # used for the count_all() and count_all_results() functions.
        self._count_string = 'SELECT COUNT(*) AS '
        self._random_keyword = ' RAND()'  # database specific random keyword

        self._cursor = None
        self.connect()

    def connect(self):
        try:
            if self._dbuser is not None and self._dbpass is not None:
                self._conn = MySQLdb.connect(host=self._dbhost, user=self._dbuser,
                                             passwd=self._dbpass)
            elif self._dbuser is not None:
                self._conn = MySQLdb.connect(host=self._dbhost, user=self._dbuser)
            else:
                self._conn = MySQLdb.connect(host=self._dbhost)
            self._conn.database = self._dbname
            return True
        except MySQLdb.OperationalError as error:
            self._conn = None
            print("#OperationalError:%s" % error)
            return False
        except Exception:
            self._conn = None
            print(traceback.format_exc())
            return False

    def close(self):
        self._conn.close()
        self._alive = False

    def ping(self):
        try:
            if self._conn:
                self._conn.ping()
                self._cursor = self._conn.cursor()
                return True
            else:
                print("#_conn Type Error, reconnect")
                self.connect()
                return False
        except MySQLdb.OperationalError as error:
            print("#OperationalError:%s, reconnect." % error)
            self.connect()
            return False
        except Exception:
            print(traceback.format_exc())
            self.connect()
            return False

    def mysql_real_escape_string(self, string):
        if self._conn:
            pass
            # string = self._conn.escape_string(string)
        else:
            string = ''.join({'"': '\\"',
                              "'": "\\'",
                              "\0": "\\\0",
                              "\\": "\\\\"}.get(c, c) for c in string)
        return string

    def _has_operator(self, str):
        """
         * Tests whether the string has an SQL operator
         *
         * @access  private
         * @param   string
         * @return  bool
        """

        str = str.strip()
        match = re.search("(\s|<|>|!|=|is null|is not null)", str, re.IGNORECASE)

        if not match:
            return False
        else:
            return True

    def _track_aliases(self, table):
        """
         * Track Aliases
         *
         * Used to track SQL statements written with aliased tables.
         *
         * @param   string  The table to inspect
         * @return  string
        """

        if isinstance(table, list):
            for t in table:
                self._track_aliases(t)
            return

        # Does the string contain a comma?  If so, we need to separate
        # the string into discreet statements
        if ',' in table:
            return self._track_aliases(table.split(','))

        # if a table alias is used we can recognize it by a space
        if ' ' in table:
            # if the alias is written with the AS keyword, remove it
            table = re.sub('\s+AS\s+', ' ', table, flags=re.IGNORECASE)

            # Grab the alias
            table = table[table.rfind(' '):].strip()

            # Store the alias, if it doesn't already exist
            if table not in self.ar_aliased_tables:
                self.ar_aliased_tables.append(table)

    def _merge_cache(self):
        """
         * Merge Cache
         *
         * When called, this function merges any cached AR arrays with
         * locally called ones.
         *
         * @return  void
        """

        if len(self.ar_cache_exists) == 0:
            return

        for val in self.ar_cache_exists:
            ar_variable    = 'ar_' + val
            ar_cache_var   = 'ar_cache_' + val

            if len(getattr(self, ar_cache_var)) == 0:
                continue

            if isinstance(getattr(self, ar_variable), list):
                setattr(self, ar_variable,
                        list(set(getattr(self, ar_cache_var) + getattr(self, ar_variable))))
            elif isinstance(self.ar_variable, dict):
                setattr(self, ar_variable,
                        list(set(dict(getattr(self, ar_cache_var), **getattr(self, ar_variable)))))

        # If we are "protecting identifiers" we need to examine the "from"
        # portion of the query to determine if there are any aliases
        if self.__protect_identifiers == True and len(self.ar_cache_from) > 0:
            self._track_aliases(self.ar_from)

        self.ar_no_escape = self.ar_cache_no_escape

    def _compile_select(self, select_override=False):
        """
         * Compile the SELECT statement
         *
         * Generates a query string based on which functions were used.
         * Should not be called directly.  The get() function calls it.
         *
         * @return  string
        """

        # Combine any cached components with the current statements
        # self._merge_cache()

        # ----------------------------------------------------------------

        # Write the "select" portion of the query

        if select_override != False:
            sql = select_override
        else:
            sql = 'SELECT ' if not self.ar_distinct else 'SELECT DISTINCT '

            if len(self.ar_select) == 0:
                sql += '*'
            else:
                # Cycle through the "select" portion of the query and prep each column name.
                # The reason we protect identifiers here rather then in the select() function
                # is because until the user calls the from() function we don't know if there
                # are aliases
                for i in range(len(self.ar_select)):
                    try:
                        no_escape = self.ar_no_escape[i]
                    except IndexError:
                        no_escape = None

                    self.ar_select[i] = self._protect_identifiers(self.ar_select[i],
                                                                  False, no_escape)
                sql += ', '.join(self.ar_select)

        # ----------------------------------------------------------------

        # Write the "FROM" portion of the query

        if len(self.ar_from) > 0:
            sql += "\n FROM "

            sql += self._from_tables(self.ar_from)

        # ----------------------------------------------------------------

        # Write the "JOIN" portion of the query

        if len(self.ar_join) > 0:
            sql += "\n"

            sql += "\n".join(self.ar_join)

        # ----------------------------------------------------------------

        # Write the "WHERE" portion of the query

        if len(self.ar_where) > 0 or len(self.ar_like) > 0:
            sql += "\nWHERE "

        sql += "\n".join(self.ar_where)

        # ----------------------------------------------------------------

        # Write the "LIKE" portion of the query

        if len(self.ar_like) > 0:
            if len(self.ar_where) > 0:
                sql += "\nAND "

            sql += "\n".join(self.ar_like)

        # ----------------------------------------------------------------

        # Write the "GROUP BY" portion of the query

        if len(self.ar_groupby) > 0:
            sql += "\nGROUP BY "

            sql += ', '.join(self.ar_groupby)

        # ----------------------------------------------------------------

        # Write the "HAVING" portion of the query

        if len(self.ar_having) > 0:
            sql += "\nHAVING "

            sql += "\n".join(self.ar_having)

        # ----------------------------------------------------------------

        # Write the "ORDER BY" portion of the query

        if len(self.ar_orderby) > 0:
            sql += "\nORDER BY "
            sql += ', '.join(self.ar_orderby)

            if self.ar_order != False:
                sql += ' DESC' if self.ar_order.lower() == 'desc' else ' ASC'

        # ----------------------------------------------------------------

        # Write the "LIMIT" portion of the query
        bool1 = (isinstance(self.ar_limit, int) and self.ar_limit > 0)
        bool2 = (isinstance(self.ar_limit, str) and self.ar_limit.isdigit())
        if bool1 or bool2:
            sql += "\n"
            sql = self._limit(sql, int(self.ar_limit), int(self.ar_offset))

        return sql

    def compile_binds(self, sql, binds):
        """
         * Compile Bindings
         *
         * @access  public
         * @param   string  the sql statement
         * @param   array   an array of bind data
         * @return  string
        """

        if self.bind_marker not in sql:
            return sql

        if not isinstance(binds, list):
            binds = list(binds)

        # Get the sql segments around the bind markers
        segments = sql.split(self.bind_marker)

        # The count of bind should be 1 less then the count of segments
        # If there are more bind arguments trim it down
        if len(binds) >= len(segments):
            binds = binds[0:len(segments) - 1]

        # Construct the binded query
        result = segments[0]
        i = 0
        for bind in binds:
            result += self.escape(bind)
            i += 1
            result += segments[i]

        return result

    def total_queries(self):
        """
         * Returns the total number of queries
         *
         * @access  public
         * @return  integer
        """

        return self.query_count

    def escape(self, string):
        """
         * "Smart" Escape String
         *
         * Escapes data based on type
         * Sets boolean and null types
         *
         * @access  public
         * @param   string
         * @return  mixed
        """

        if isinstance(string, str):
            string = "'" + self.escape_str(string) + "'"
        elif isinstance(string, bool):
            string = 0 if string == False else 1
        elif string is None:
            string = 'NULL'

        return string

    def escape_like_str(self, str):
        """
         * Escape LIKE String
         *
         * Calls the individual driver for platform
         * specific escaping for LIKE conditions
         *
         * @access  public
         * @param   string
         * @return  mixed
        """

        return self.escape_str(str, True)

    def escape_str(self, string, like=False):
        """
         * Escape String
         *
         * @access  public
         * @param   string
         * @param   bool    whether or not the string will be used in a LIKE condition
         * @return  string
        """

        if isinstance(string, dict):
            for key, val in string.iteritems():
                string[key] = self.escape_str(val, like)
            return string

        if self._conn:
            string = self.mysql_real_escape_string(string)
        else:
            string = ''.join({'"': '\\"',
                              "'": "\\'",
                              "\0": "\\\0",
                              "\\": "\\\\"}.get(c, c) for c in string)

        # escape LIKE condition wildcards
        if like == True:
            string = string.replace('%', '\\%')
            string = string.replace('_', '\\_')

        return string

    def _escape_identifiers(self, item):
        """
         * Escape the SQL Identifiers
         *
         * This function escapes column and table names
         *
         * @access  private
         * @param   string
         * @return  string
        """

        if self._escape_char == '':
            return item

        for id in self._reserved_identifiers:
            if '.' + id in item:
                str = self._escape_char + item.replace('.', self._escape_char + '.')

                # remove duplicates if the user already included the escape
                return re.sub('[' + self._escape_char + ']+', self._escape_char, item)

        if '.' in item:
            str = self._escape_char + \
                item.replace('.', self._escape_char + '.' + self._escape_char) + \
                self._escape_char
        else:
            str = self._escape_char + item + self._escape_char

        # remove duplicates if the user already included the escape
        return re.sub('[' + self._escape_char + ']+', self._escape_char, str)

    def _from_tables(self, tables):
        """
         * From Tables
         *
         * This function implicitly groups FROM tables so there is no confusion
         * about operator precedence in harmony with SQL standards
         *
         * @access  public
         * @param   type
         * @return  type
        """
        if isinstance(tables, list):
            return '(' + ', '.join(tables) + ')'
        elif isinstance(tables, str):
            return tables
        else:
            raise TypeError  # unknow tables name type

    def _protect_identifiers(self, item, prefix_single=False,
                             protect_identifiers=None, field_exists=True):
        """
         * Protect Identifiers
         *
         * This function is used extensively by the Active Record class, and by
         * a couple functions in this class.
         * It takes a column or table name (optionally with an alias) and inserts
         * the table prefix onto it.  Some logic is necessary in order to deal with
         * column names that include the path.  Consider a query like this:
         *
         * SELECT * FROM hostname.database.table.column AS c FROM hostname.database.table
         *
         * Or a query with aliasing:
         *
         * SELECT m.member_id, m.member_name FROM members AS m
         *
         * Since the column name can include up to four segments (host, DB, table, column)
         * or also have an alias prefix, we need to do a bit of work to figure this out and
         * insert the table prefix (if it exists) in the proper position, and escape only
         * the correct identifiers.
         *
         * @access  private
         * @param   string
         * @param   bool
         * @param   mixed
         * @param   bool
         * @return  string
        """
        if not isinstance(protect_identifiers, bool):
            protect_identifiers = self._protect_identifiers

        if isinstance(item, list):
            escaped_array = []

            for k, v in item.iteritems():
                escaped_array[self._protect_identifiers(k)] = self._protect_identifiers(v)

            return escaped_array

        # Convert tabs or multiple spaces into single spaces
        item = re.sub('[\t ]+', ' ', item)

        # If the item has an alias declaration we remove it and set it aside.
        # Basically we remove everything to the right of the first space
        pos = item.find(' ')
        if pos >= 0:
            alias = item[pos:]
            item = item[0:- len(alias)]
        else:
            alias = ''

        # This is basically a bug fix for queries that use MAX, MIN, etc.
        # If a parenthesis is found we know that we do not need to
        # escape the data or add a prefix.  There's probably a more graceful
        # way to deal with this, but I'm not thinking of it -- Rick
        if '(' in item:
            return item + alias

        # there a table prefix?  If not, no need to insert it
        if self.dbprefix != '':
            # table prefix and replace if necessary
            if self.swap_pre != '' and cmp(item[:len(self.swap_pre)],
                                           self.swap_pre[:len(self.swap_pre)]) == 0:
                item = re.sub("^" + self.swap_pre + "(\S+?)", self.dbprefix + "\1", item)

            # we prefix an item with no segments?
            if prefix_single == True and item[0:len(self.dbprefix)] != self.dbprefix:
                item = self.dbprefix + item

        if protect_identifiers == True and item not in self._reserved_identifiers:
            item = self._escape_identifiers(item)

        return item + alias

    def select(self, select='*', escape=None):
        """
         * Select
         *
         * Generates the SELECT portion of the query
         *
         * @param   string
         * @return  object
        """

        if isinstance(select, str):
            select = select.split(',')

        for val in select:
            val = val.strip()

            if val != '':
                self.ar_select.append(val)
                self.ar_no_escape.append(escape)

                if self.ar_caching is True:
                    self.ar_cache_select.append(val)
                    self.ar_cache_exists.append('select')
                    self.ar_cache_no_escape.append(escape)

        return self

    def select_max(self, select='', alias=''):
        """
         * Select Max
         *
         * Generates a SELECT MAX(field) portion of a query
         *
         * @param   string  the field
         * @param   string  an alias
         * @return  object
        """

        return self._max_min_avg_sum(select, alias, 'MAX')

    def select_min(self, select='', alias=''):
        """
         * Select Min
         *
         * Generates a SELECT MIN(field) portion of a query
         *
         * @param   string  the field
         * @param   string  an alias
         * @return  object
        """

        return self._max_min_avg_sum(select, alias, 'MIN')

    def select_avg(self, select='', alias=''):
        """
         * Select Average
         *
         * Generates a SELECT AVG(field) portion of a query
         *
         * @param   string  the field
         * @param   string  an alias
         * @return  object
        """

        return self._max_min_avg_sum(select, alias, 'AVG')

    def select_sum(self, select='', alias=''):
        """
         * Select Sum
         *
         * Generates a SELECT SUM(field) portion of a query
         *
         * @param   string  the field
         * @param   string  an alias
         * @return  object
        """

        return self._max_min_avg_sum(select, alias, 'SUM')

    def _max_min_avg_sum(self, select='', alias='', type='MAX'):
        """
         * Processing Function for the four functions above:
         *
         *  select_max()
         *  select_min()
         *  select_avg()
         *  select_sum()
         *
         * @param   string  the field
         * @param   string  an alias
         * @return  object
        """

        if not isinstance(select, str) or select == '':
            print('db_invalid_query')

        type = type.upper()

        if type not in ['MAX', 'MIN', 'AVG', 'SUM']:
            raise ValueError('Invalid function type: %s' % type)

        if alias == '':
            alias = self._create_alias_from_table(select.strip())

        sql = type + '(' + self._protect_identifiers(select.strip()) + ') AS ' + alias

        self.ar_select.append(sql)

        if self.ar_caching == True:
            self.ar_cache_select.append(sql)
            self.ar_cache_exists.append('select')

        return self

    def _create_alias_from_table(self, item):
        """
         * Determines the alias name based on the table
         *
         * @param   string
         * @return  string
        """

        if '.' in item:
            return item.split('.')[:-1]

        return item

    def distinct(self, val=True):
        """
         * DISTINCT
         *
         * Sets a flag which tells the query string compiler to add DISTINCT
         *
         * @param   bool
         * @return  object
        """

        self.ar_distinct = val if isinstance(val, bool) else True
        return self

    def table(self, table):
        """
         * Table
         *
         * Generates the FROM portion of the query
         *
         * @param   mixed   can be a string or array
         * @return  object
        """

        table = table.strip()

        # any aliases that might exist.  We use this information
        # the _protect_identifiers to know whether to add a table prefix
        self._track_aliases(table)

        self.ar_from.append(self._protect_identifiers(table, True, None, False))

        if self.ar_caching == True:
            self.ar_cache_from.append(self._protect_identifiers(table, True, None, False))
            self.ar_cache_exists.append('from')

        return self

    def join(self, table, cond, type=''):
        """
         * Join
         *
         * Generates the JOIN portion of the query
         *
         * @param   string
         * @param   string  the join condition
         * @param   string  the type of join
         * @return  object
        """

        if type != '':
            type = type.strip().upper()

            if type not in ['LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER']:
                type = ''
            else:
                type += ' '

        # Extract any aliases that might exist.  We use this information
        # in the _protect_identifiers to know whether to add a table prefix
        self._track_aliases(table)

        # Strip apart the condition and protect the identifiers
        match = re.search('/([\w\.]+)([\W\s]+)(.+)/', cond)
        if match:
            match[1] = self._protect_identifiers(match[1])
            match[3] = self._protect_identifiers(match[3])

            cond = match[1] + match[2] + match[3]

        # Assemble the JOIN statement
        join = type + 'JOIN ' + self._protect_identifiers(table, True, None, False) + ' ON ' + cond

        self.ar_join.append(join)
        if self.ar_caching == True:
            self.ar_cache_join.append(join)
            self.ar_cache_exists.append('join')

        return self

    def where(self, key, value=None, escape=True):
        """
         * Where
         *
         * Generates the WHERE portion of the query. Separates
         * multiple calls with AND
         *
         * @param   mixed
         * @param   mixed
         * @return  object
        """

        return self._where(key, value, 'AND ', escape)

    def or_where(self, key, value=None, escape=True):
        """
         * OR Where
         *
         * Generates the WHERE portion of the query. Separates
         * multiple calls with OR
         *
         * @param   mixed
         * @param   mixed
         * @return  object
        """

        return self._where(key, value, 'OR ', escape)

    def _where(self, key, value=None, type='AND ', escape=None):
        """
         * Where
         *
         * Called by where() or or_where()
         *
         * @param   mixed
         * @param   mixed
         * @param   string
         * @return  object
        """

        if not isinstance(key, dict):
            key = {key: value}

        # If the escape value was not set will will base it on the global setting
        if not isinstance(escape, bool):
            escape = self.__protect_identifiers

        for k, v in key.iteritems():
            # Convert int value to str value
            if isinstance(v, int):
                v = str(v)
            prefix = '' if len(self.ar_where) == 0 and len(self.ar_cache_where) == 0 else type

            if v is None and not self._has_operator(k):
                # value appears not to have been set, assign the test to IS NULL
                k += ' IS NULL'

            if v is not None:
                if escape == True:
                    k = self._protect_identifiers(k, False, escape)
                    v = ' ' + self.escape(v)

                if not self._has_operator(k):
                    k += ' = '
            else:
                k = self._protect_identifiers(k, False, escape)

            self.ar_where.append(prefix + k + v)

            if self.ar_caching == True:
                self.ar_cache_where.append(prefix + k + v)
                self.ar_cache_exists.append('where')

        return self

    def where_in(self, key=None, values=None):
        """
         * Where_in
         *
         * Generates a WHERE field IN ('item', 'item') SQL query joined with
         * AND if appropriate
         *
         * @param   string  The field to search
         * @param   array   The values searched on
         * @return  object
        """

        return self._where_in(key, values)

    def or_where_in(self, key=None, values=None):
        """
         * Where_in_or
         *
         * Generates a WHERE field IN ('item', 'item') SQL query joined with
         * OR if appropriate
         *
         * @param   string  The field to search
         * @param   array   The values searched on
         * @return  object
        """

        return self._where_in(key, values, False, 'OR ')

    def where_not_in(self, key=None, values=None):
        """
         * Where_not_in
         *
         * Generates a WHERE field NOT IN ('item', 'item') SQL query joined
         * with AND if appropriate
         *
         * @param   string  The field to search
         * @param   array   The values searched on
         * @return  object
        """

        return self._where_in(key, values, True)

    def or_where_not_in(self, key=None, values=None):
        """
         * Where_not_in_or
         *
         * Generates a WHERE field NOT IN ('item', 'item') SQL query joined
         * with OR if appropriate
         *
         * @param   string  The field to search
         * @param   array   The values searched on
         * @return  object
        """

        return self._where_in(key, values, True, 'OR ')

    def _where_in(self, key=None, values=None, not_in=False, type='AND '):
        """
         * Where_in
         *
         * Called by where_in, where_in_or, where_not_in, where_not_in_or
         *
         * @param   string  The field to search
         * @param   array   The values searched on
         * @param   boolean If the statement would be IN or NOT IN
         * @param   string
         * @return  object
        """

        if key is None or values is None:
            return

        if not isinstance(values, list):
            values = list(values)

        not_in = ' NOT' if not_in else ''

        for value in values:
            # Convert int value to str value
            if isinstance(value, int):
                value = str(value)
            self.ar_wherein.append(self.escape(value))

        prefix = '' if len(self.ar_where) == 0 else type

        where_in = prefix + self._protect_identifiers(key) + \
            not_in + " IN (" + ", ".join(self.ar_wherein) + ") "

        self.ar_where.append(where_in)
        if self.ar_caching == True:
            self.ar_cache_where.append(where_in)
            self.ar_cache_exists.append('where')

        # reset the array for multiple calls
        self.ar_wherein = []
        return self

    def like(self, field, match='', side='both'):
        """
         * Like
         *
         * Generates a %LIKE% portion of the query. Separates
         * multiple calls with AND
         *
         * @param   mixed
         * @param   mixed
         * @return  object
        """

        return self._like(field, match, 'AND ', side)

    def not_like(self, field, match='', side='both'):
        """
         * Not Like
         *
         * Generates a NOT LIKE portion of the query. Separates
         * multiple calls with AND
         *
         * @param   mixed
         * @param   mixed
         * @return  object
        """

        return self._like(field, match, 'AND ', side, 'NOT')

    def or_like(self, field, match='', side='both'):
        """
         * OR Like
         *
         * Generates a %LIKE% portion of the query. Separates
         * multiple calls with OR
         *
         * @param   mixed
         * @param   mixed
         * @return  object
        """

        return self._like(field, match, 'OR ', side)

    def or_not_like(self, field, match='', side='both'):
        """
         * OR Not Like
         *
         * Generates a NOT LIKE portion of the query. Separates
         * multiple calls with OR
         *
         * @param   mixed
         * @param   mixed
         * @return  object
        """

        return self._like(field, match, 'OR ', side, 'NOT')

    def _like(self, field, match='', type='AND ', side='both', not_like=''):
        """
         * Like
         *
         * Called by like() or orlike()
         *
         * @param   mixed
         * @param   mixed
         * @param   string
         * @return  object
        """
        if not isinstance(field, dict):
            field = {field: match}

        for k, v in field.iteritems():
            k = self._protect_identifiers(k)

            prefix = '' if len(self.ar_like) == 0 else type

            v = self.escape_like_str(v)

            if side == 'none':
                like_statement = prefix + " {0} {1} LIKE '{2}'".format(k, not_like, v)
            elif side == 'before':
                like_statement = prefix + " {0} {1} LIKE '%{2}'".format(k, not_like, v)
            elif side == 'after':
                like_statement = prefix + " {0} {1} LIKE '{2}%'".format(k, not_like, v)
            else:
                like_statement = prefix + " {0} {1} LIKE '%{2}%'".format(k, not_like, v)

            # some platforms require an escape sequence definition for LIKE wildcards
            if self._like_escape_str != '':
                like_statement = like_statement % (self._like_escape_str, self._like_escape_chr)

            self.ar_like.append(like_statement)
            if self.ar_caching == True:
                self.ar_cache_like.append(like_statement)
                self.ar_cache_exists.append('like')

        return self

    def group_by(self, by):
        """
         * GROUP BY
         *
         * @param   string
         * @return  object
        """
        if isinstance(by, str):
            by = by.split(',')

        for val in by:
            val = val.strip()

            if val != '':
                self.ar_groupby.append(self._protect_identifiers(val))

                if self.ar_caching == True:
                    self.ar_cache_groupby.append(self._protect_identifiers(val))
                    self.ar_cache_exists.append('groupby')
        return self

    def having(self, key, value='', escape=True):
        """
         * Sets the HAVING value
         *
         * Separates multiple calls with AND
         *
         * @param   string
         * @param   string
         * @return  object
        """

        return self._having(key, value, 'AND ', escape)

    def or_having(self, key, value='', escape=True):
        """
         * Sets the OR HAVING value
         *
         * Separates multiple calls with OR
         *
         * @param   string
         * @param   string
         * @return  object
        """

        return self._having(key, value, 'OR ', escape)

    def _having(self, key, value='', type='AND ', escape=True):
        """
         * Sets the HAVING values
         *
         * Called by having() or or_having()
         *
         * @param   string
         * @param   string
         * @return  object
        """

        if not isinstance(key, dict):
            key = {key: value}

        for k, v in key.iteritems():

            prefix = '' if len(self.ar_having) == 0 else type

            if escape == True:
                k = self._protect_identifiers(k)

            if not self._has_operator(k):
                k += ' = '

            if v != '':
                v = ' ' + self.escape(v)

            self.ar_having.append(prefix + k + v)
            if self.ar_caching == True:
                self.ar_cache_having.append(prefix + k + v)
                self.ar_cache_exists.append('having')

        return self

    def order_by(self, orderby, direction=''):
        """
         * Sets the ORDER BY value
         *
         * @param   string
         * @param   string  direction: asc or desc
         * @return  object
        """

        if direction.lower() == 'random':
            orderby = ''  # Random results want or don't need a field name
            direction = self._random_keyword
        elif direction.strip() != '':
            direction = ' ' + direction if direction.strip().upper() in ['ASC', 'DESC'] else ' ASC'

        if ',' in orderby:
            temp = []
            for part in orderby.split(','):
                part = part.strip()
                if part not in self.ar_aliased_tables:
                    part = self._protect_identifiers(part)
                temp.append(part)

            orderby = ', '.join(temp)
        elif direction != self._random_keyword:
            orderby = self._protect_identifiers(orderby)

        orderby_statement = orderby + direction

        self.ar_orderby.append(orderby_statement)
        if self.ar_caching == True:
            self.ar_cache_orderby.append(orderby_statement)
            self.ar_cache_exists.append('orderby')

        return self

    def limit(self, value, offset=''):
        """
         * Sets the LIMIT value
         *
         * @param   integer the limit value
         * @param   integer the offset value
         * @return  object
        """

        self.ar_limit = int(value)

        if offset != '':
            self.ar_offset = int(offset)

        return self

    def offset(self, offset):
        """
         * Sets the OFFSET value
         *
         * @param   integer the offset value
         * @return  object
        """

        self.ar_offset = int(offset)
        return self

    def _limit(self, sql, limit, offset):
        """
         * Limit string
         *
         * Generates a platform-specific LIMIT clause
         *
         * @access  public
         * @param   string  the sql query string
         * @param   integer the number of rows to limit the query to
         * @param   integer the offset value
         * @return  string
        """

        if offset == 0:
            offset = ''
        else:
            offset += ", "

        return sql + "LIMIT " + offset + str(limit)

    def set(self, key, value='', escape=True):
        """
         * The "set" function.  Allows key/value pairs to be set for inserting or updating
         *
         * @param   mixed
         * @param   string
         * @param   boolean
         * @return  object
        """
        if not isinstance(key, dict):
            key = {key: value}

        for k, v in key.iteritems():
            # Convert int value to str value
            if v and isinstance(v, int):
                v = str(v)
            if escape == False:
                self.ar_set[self._protect_identifiers(k)] = v
            else:
                self.ar_set[self._protect_identifiers(k, False, True)] = self.escape(v)

        return self

    def get(self, table='', limit=None, offset=None):
        """
         * Get
         *
         * Compiles the select statement based on the other functions called
         * and runs the query
         *
         * @param   string  the table
         * @param   string  the limit clause
         * @param   string  the offset clause
         * @return  object
        """

        if table != '':
            self._track_aliases(table)
            self.table(table)

        if limit:
            self.limit(limit, offset)

        sql = self._compile_select()
        if self._dbug:
            print(sql)

        self.query(sql)

        self._reset_select()

        return self

    def count_all_results(self, table=''):
        """
         * "Count All Results" query
         *
         * Generates a platform-specific query string that counts all records
         * returned by an Active Record query.
         *
         * @param   string
         * @return  string
        """

        if table != '':
            self._track_aliases(table)
            self.table(table)

        sql = self._compile_select(self._count_string + self._protect_identifiers('numrows'))

        self.query(sql)
        self._reset_select()

        if self.num_rows() == 0:
            return 0

        row = self.row()
        return int(row.numrows)

    def get_where(self, table='', where=None, limit=None, offset=None):
        """
         * Get_Where
         *
         * Allows the where clause, limit and offset to be added directly
         *
         * @param   string  the where clause
         * @param   string  the limit clause
         * @param   string  the offset clause
         * @return  object
        """

        if table != '':
            self.table(table)

        if where is not None:
            self.where(where)

        if limit is not None:
            self.limit(limit, offset)

        sql = self._compile_select()

        self.query(sql)
        self._reset_select()
        return self

    def query(self, sql, binds=False, return_object=True):
        """
         * Execute the query
         *
         * Accepts an SQL string as input and returns a result object upon
         * successful execution of a "read" type query. Returns boolean True
         * upon successful execution of a "write" type query. Returns boolean
         * False upon failure, and if the $db_debug variable is set to True
         * will raise an error.
         *
         * @access  public
         * @param   string  An SQL query string
         * @param   array   An array of binding data
         * @return  mixed
        """

        if sql == '':
            if self.db_debug:
                print('db_invalid_query')
            return False

        # Verify table prefix and replace if necessary
        if (self.dbprefix != '' and self.swap_pre != '') and (self.dbprefix != self.swap_pre):
            sql = re.sub("(\W)" + self.swap_pre + "(\S+?)", "\1" + self.dbprefix + "\2", sql)

        # Compile binds if needed
        if binds != False:
            sql = self.compile_binds(sql, binds)

        # Save the  query for debugging
        if self.save_queries == True:
            self.queries.append(sql)

        # Start the Query Timer
        (sm, ss) = microtime().split(' ')

        # Run the Query
        query_result = self.simple_query(sql)
        if query_result == False:
            if self.save_queries == True:
                self.query_times.append(0)

            # This will trigger a rollback if transactions are being used
            self._trans_status = False

            return False

        # Stop and aggregate the query time results
        (em, es) = microtime().split(' ')
        self.benchmark += (float(em) + float(es)) - (float(sm) + float(ss))

        if self.save_queries == True:
            self.query_times.append((float(em) + float(es)) - (float(sm) + float(ss)))

        # Increment the query counter
        self.query_count += 1

        # Return True if we don't need to create a result object
        # Currently only the Oracle driver uses this when stored
        # procedures are used
        if return_object != True:
            return True

        return self._cursor

    def simple_query(self, sql):
        """
         * Simple Query
         * This is a simplified version of the query() function.  Internally
         * we only use it when running transaction commands since they do
         * not require all the features of the main query() function.
         *
         * @access  public
         * @param   string  the sql query
         * @return  mixed
        """

        try:
            if self.ping():
                if self._cursor:
                    self._execute(sql)
                    return True
        except MySQLdb.OperationalError as error:
            print("#OperationalError:%s" % error)
            return False
        except MySQLdb.IntegrityError as error:
            print("#IntegrityError:%s" % error)
            return False
        except Exception:
            print(traceback.format_exc())
            return False

    def _execute(self, sql):
        """
         * Execute the query
         *
         * @access  private called by the base class
         * @param   string  an SQL query
         * @return  resource
        """
        sql = self._prep_query(sql)
        self._cursor.execute(sql)

    def _prep_query(self, sql):
        """
         * Prep the query
         *
         * If needed, each database adapter can prep the query string
         *
         * @access  private called by execute()
         * @param   string  an SQL query
         * @return  string
        """

        # "DELETE FROM TABLE" returns 0 affected rows This hack modifies
        # the query so that it returns the number of affected rows
        if self.delete_hack == True:
            if re.search('^\s*DELETE\s+FROM\s+(\S+)\s*$', sql, re.IGNORECASE):
                sql = re.sub('^\s*DELETE\s+FROM\s+(\S+)\s*$', "DELETE FROM \1 WHERE 1=1", sql)

        return sql

    def cache_set_path(self, path=''):
        """
         * Set Cache Directory Path
         *
         * @access  public
         * @param   string  the path to the cache directory
         * @return  void
        """

        self.cachedir = path

    def cache_on(self):
        """
         * Enable Query Caching
         *
         * @access  public
         * @return  void
        """

        self.cache_on = True
        return True

    def cache_off(self):
        """
         * Disable Query Caching
         *
         * @access  public
         * @return  void
        """

        self.cache_on = False
        return False

    def cache_delete(self, segment_one='', segment_two=''):
        """
         * Delete the cache files associated with a particular URI
         *
         * @access  public
         * @return  void
        """
        if not self._cache_init():
            return False
        return self.CACHE.delete(segment_one, segment_two)

    def cache_delete_all(self):
        """
         * Delete All cache files
         *
         * @access  public
         * @return  void
        """
        if not self._cache_init():
            return False

        return self.CACHE.delete_all()

    def _cache_init(self):
        """
         * Initialize the Cache Class
         *
         * @access  private
         * @return  void
        """

        return self.cache_off()

    def _reset_run(self, ar_reset_items):
        """
         * Resets the active record values.  Called by the get() function
         *
         * @param   array   An array of fields to reset
         * @return  void
        """
        for item, default_value in ar_reset_items.iteritems():
            if item not in self.ar_store_array:
                setattr(self, item, default_value)

    def _reset_select(self):
        """
         * Resets the active record values.  Called by the get() function
         *
         * @return  void
        """

        ar_reset_items = {
            'ar_select'         : [],
            'ar_from'           : [],
            'ar_join'           : [],
            'ar_where'          : [],
            'ar_like'           : [],
            'ar_groupby'        : [],
            'ar_having'         : [],
            'ar_orderby'        : [],
            'ar_wherein'        : [],
            'ar_aliased_tables' : [],
            'ar_no_escape'      : [],
            'ar_distinct'       : False,
            'ar_limit'          : False,
            'ar_offset'         : False,
            'ar_order'          : False,
        }

        self._reset_run(ar_reset_items)

    def _reset_write(self):
        """
         * Resets the active record "write" values.
         *
         * Called by the insert() update() insert_batch() update_batch() and delete() functions
         *
         * @return  void
        """

        ar_reset_items = {
            'ar_set'        : {},
            'ar_from'       : [],
            'ar_where'      : [],
            'ar_like'       : [],
            'ar_orderby'    : [],
            'ar_keys'       : [],
            'ar_limit'      : False,
            'ar_order'      : False
        }

        self._reset_run(ar_reset_items)

    def affected_rows(self):
        """
         * Affected Rows
         *
         * @access  public
         * @return  integer
        """

        data = None
        try:
            if self._cursor:
                data = self._cursor.rowcount
        except Exception:
            print(traceback.format_exc())
        finally:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            return data

    def insert_id(self):
        """
         * Insert ID
         *
         * @access  public
         * @return  integer
        """

        data = None
        try:
            if self._cursor:
                data = self._cursor.lastrowid
        except Exception:
            print(traceback.format_exc())
        finally:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            return data

    def num_rows(self):
        """
         * Number of rows in the result set
         *
         * @access  public
         * @return  integer
        """

        data = None
        try:
            if self._cursor:
                data = self._cursor.rowcount
        except Exception:
            print(traceback.format_exc())
        finally:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            return data

    def row(self):
        data = None
        try:
            if self._cursor:
                data = self._cursor.fetchone()
        except Exception:
            print(traceback.format_exc())
        finally:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            return data

    def result(self):
        data = None
        try:
            if self._cursor:
                data = self._cursor.fetchall()
        except Exception:
            print(traceback.format_exc())
        finally:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            return data


def microtime(get_as_float=False) :
    if get_as_float:
        return time.time()
    else:
        return '%f %d' % math.modf(time.time())
