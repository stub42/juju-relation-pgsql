# Copyright 2016 Canonical Ltd.
#
# This file is part of the PostgreSQL Client Interface for Juju charms.reactive
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from collections import namedtuple
import ipaddress
import itertools
import urllib.parse

from charmhelpers import context
from charmhelpers.core import hookenv
from charms.reactive import hook, scopes, RelationBase


# This data structure cannot be in an external library, as interfaces
# have no way to declare dependencies. It also must be defined in this
# file, as reactive framework imports are broken per
# https://github.com/juju-solutions/charms.reactive/pull/51
#
class ConnectionString(str):
    """A libpq connection string.

    >>> c = ConnectionString(host='1.2.3.4', dbname='mydb',
    ...                      port=5432, user='anon', password='secret')
    ...
    >>> c
    'host=1.2.3.4 dbname=mydb port=5432 user=anon password=secret

    Components may be accessed as attributes.

    >>> c.dbname
    'mydb'
    >>> c.host
    '1.2.3.4'
    >>> c.port
    '5432'

    The standard URI format is also accessible:

    >>> c.uri
    'postgresql://anon:secret@1.2.3.4:5432/mydb'

    """
    def __new__(self, **kw):
        def quote(x):
            return str(x).replace("\\", "\\\\").replace("'", "\\'")
        c = " ".join("{}={}".format(k, quote(v)) for k, v in kw.items())
        c = str.__new__(self, c)

        for k, v in kw.items():
            setattr(c, k, v)

        self._keys = set(kw.keys())

        d = {k: urllib.parse.quote(v, safe='') for k, v in kw.items()}
        try:
            hostaddr = ipaddress.ip_address(kw.get('hostaddr') or
                                            kw.get('host'))
            if isinstance(hostaddr, ipaddress.IPv6Address):
                d['host'] = '[{}]'.format(hostaddr)
            else:
                d['host'] = str(hostaddr)
        except ValueError:
            pass
        fmt = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'
        self.uri = fmt.format(**d)

        return c

    host = None
    dbname = None
    port = None
    user = None
    password = None
    uri = None

    def keys(self):
        return iter(self._keys)

    def items(self):
        return {k: self[k] for k in self.keys()}.items()

    def values(self):
        return iter(self[k] for k in self.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return super(ConnectionString, self).__getitem__(key)
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)


ConnectionStrings = namedtuple('ConnectionStrings',
                               ['relid', 'master', 'standbys'])
ConnectionStrings.__doc__ = (
    """Collection of :class:`ConnectionString` for a relation.""")
ConnectionStrings.relid.__doc__ = 'Relation id'
ConnectionStrings.master.__doc__ = 'master database :class:`ConnectionString`'
ConnectionStrings.standbys.__doc__ = (
    'set of :class:`ConnectionString` to hot standby databases')


class PostgreSQLClient(RelationBase):
    """
    PostgreSQL client interface.

    A client may be related to one or more PostgreSQL services.

    In most cases, a charm will only use a single PostgreSQL
    service being related for each relation defined in metadata.yaml
    (so one per relation name). To access the connection strings, use
    the master and standbys attributes::

        @when('productdb.master.available')
        def setup_database(pgsql):
            conn_str = pgsql.master  # A ConnectionString.
            update_db_conf(conn_str)

        @when('productdb.standbys.available')
        def setup_cache_databases(pgsql):
            set_cache_db_list(pgsql.standbys)  # set of ConnectionString.

    In somecases, a relation name may be related to several PostgreSQL
    services. You can also access the ConnectionStrings for a particular
    service by relation id or by iterating over all of them::

        @when('db.master.available')
        def set_dbs(pgsql):
            update_monitored_dbs(cs.master
                                 for cs in pgsql  # ConnectionStrings.
                                 if cs.master)
    """
    scope = scopes.SERVICE

    @hook('{requires:pgsql}-relation-joined')
    def joined(self):
        # There is at least one named relation
        self.set_state('{relation_name}.connected')
        hookenv.log('Joined {} relation'.format(hookenv.relation_id()))

    @hook('{requires:pgsql}-relation-{joined,changed,departed}')
    def changed(self):
        relid = hookenv.relation_id()
        cs = self[relid]

        # There is a master in this relation.
        self.toggle_state('{relation_name}.master.available',
                          cs.master)

        # There is at least one standby in this relation.
        self.toggle_state('{relation_name}.standbys.available',
                          cs.standbys)

        # There is at least one database in this relation.
        self.toggle_state('{relation_name}.database.available',
                          cs.master or cs.standbys)

        # Ideally, we could turn logging off using a layer option
        # but that is not available for interfaces.
        if cs.master and cs.standbys:
            hookenv.log('Relation {} has master and standby '
                        'databases available'.format(relid))
        elif cs.master:
            hookenv.log('Relation {} has a master database available, '
                        'but no standbys'.format(relid))
        elif cs.standbys:
            hookenv.log('Relation {} only has standby databases '
                        'available'.format(relid))
        else:
            hookenv.log('Relation {} has no databases available'.format(relid))

    @hook('{requires:pgsql}-relation-departed')
    def departed(self):
        if not any(u for u in hookenv.related_units() or []
                   if u != hookenv.remote_unit()):
            self.remove_state('{relation_name}.connected')
            self.conversation().depart()
            hookenv.log('Departed {} relation'.format(hookenv.relation_id()))

    def set_database(self, dbname, relid=None):
        """Set the database that the named relations connect to.

        The PostgreSQL service will create the database if necessary. It
        will never remove it.

        :param dbname: The database name. If unspecified, the local service
                       name is used.

        :param relid: relation id to send the database name setting to.
                      If unset, the setting is broadcast to all relations
                      sharing the relation name.

        """
        for c in self.conversations():
            if relid is None or c.namespace == relid:
                c.set_remote('database', dbname)

    def __getitem__(self, relid):
        """:returns: :class:`ConnectionStrings` for the relation id."""
        relations = context.Relations()
        relname = self.relation_name
        assert relid.startswith('{}:'.format(relname)), (
            'relid {} not handled by {} instance'.format(relid, relname))

        relation = relations[relname][relid]

        master_reldatas = [reldata
                           for reldata in relation.values()
                           if reldata.get('state') in ('master', 'standalone')]
        if len(master_reldatas) == 1:
            master_reldata = master_reldatas[0]  # One, and only one.
        else:
            # None ready, or multiple due to failover in progress.
            master_reldata = None

        master = self._cs(master_reldata)

        standbys = set(filter(None,
                              (self._cs(reldata)
                               for reldata in relation.values()
                               if reldata.get('state') == 'hot standby')))

        return ConnectionStrings(relid, master, standbys)

    def __iter__(self):
        """:returns: Iterator of :class:`ConnectionStrings` for this named
                     relation, one per relation id.
        """
        return iter(self[relid]
                    for relid in context.Relations()[self.relation_name])

    @property
    def master(self):
        ''':class:`ConnectionString` to the master, or None.

        If multiple PostgreSQL services are related using this relation
        name then the first master found is returned.
        '''
        for cs in self:
            if cs.master:
                return cs.master

    @property
    def standbys(self):
        '''Set of class:`ConnectionString` to the read-only hot standbys.

        If multiple PostgreSQL services are related using this relation
        name then all standbys found are returned.
        '''
        return set(itertools.chain(*(cs.standbys for cs in self)))

    def connection_string(self, unit=None):
        ''':class:`ConnectionString` to the remote unit, or None.

        unit defaults to the active remote unit.

        You should normally use the master or standbys attributes rather
        than this method.

        If the unit is related multiple times using the same relation
        name, the first one found is returned.
        '''
        if unit is None:
            unit = hookenv.remote_unit()

        relations = context.Relations()
        for relation in relations[self.relation_name].values():
            if unit in relation:
                conn_str = self._cs(relation[unit])
                if conn_str:
                    return conn_str
        raise LookupError(unit)

    def _cs(self, reldata):
        if not reldata:
            return None
        d = dict(host=reldata.get('host'),
                 port=reldata.get('port'),
                 dbname=reldata.get('database'),
                 user=reldata.get('user'),
                 password=reldata.get('password'))
        if not all(d.values()):
            return None
        local_unit = hookenv.local_unit()
        if local_unit not in reldata.get('allowed-units', '').split():
            # Not yet authorized
            return None
        locdata = context.Relations()[reldata.relname][reldata.relid].local
        if 'database' in locdata and locdata['database'] != d['dbname']:
            # Requested database does not yet match
            return None
        return ConnectionString(**d)
