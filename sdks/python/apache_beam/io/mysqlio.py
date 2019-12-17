#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""This module implements IO classes to read and write data to MySQL.


Read from MySQL
-----------------
:class:`ReadFromMysql` is a ``PTransform`` that reads from a configured
MySQL source and returns a ``PCollection`` of dict representing MySQL
rows.
To configure MySQL source, the username, password, host, database, and
destination table needs to be provided.
Example usage::
  pipeline | ReadFromMySQL(username='user',
                           password='password',
                           host='localhost',
                           port=3306,
                           database='testdb',
                           table='output',
                           columns=['col_one', 'col_two'])


Write to MySQL:
-----------------
:class:`WriteToMysql` is a ``PTransform`` that writes MySQL rows to
configured sink, and the write is conducted through a MySQL executemany.
Example usage::
  pipeline | WriteToMySQL(username='user',
                          password='password',
                          host='localhost',
                          port=3306,
                          database='testdb',
                          table='output',
                          batch_size=1000)


No backward compatibility guarantees. Everything in this module is experimental.
"""
import json
import logging
import warnings

import numpy as np
import pymysql

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OrderedPositionRangeTracker, UnsplittableRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.utils.annotations import experimental

_LOGGER = logging.getLogger(__name__)

__all__ = ['ReadFromMysql', 'WriteToMysql']


@experimental()
class ReadFromMysql(PTransform):
  """A ``PTransfrom`` to read MySQL rows into a ``PCollection``.
  """

  def __init__(self,
               user, # type: str
               password, # type: str
               host, # type: str
               database, # type: str
               table, # type: str
               port=3306, # type: Optional[int]
               columns=None,  # type: List[str]
               extra_client_params=None, # type: Optional[Dict]
               ordering_col=None, # type: Optional[str]
               ):
    """ Constructor of the Read connector of MySQL

    Args:
      user(str): MySQL user to connect with
      password(str): MySQL password to connect with
      host(str): MySQL host to connect to
      database(str): Name of the database on the MySQL database server to connect to
      table_(str): MySQL Table to write the `DirectRows`
      port(int): Optional MySQL port to connect to, defaults to standard MySQL port 3306
      columns (List[str]): A list of columns to select from the table
      extra_client_params(dict): Optional `pymysql.connect
        https://pymysql.readthedocs.io/en/latest/modules/connections.html` parameters as
        keyword arguments
      ordering_col(str): Optional column to use for ordering if there is no autoincrementing
       primary key. Warning, if data is written to the database when using this, the data
       might be missed.

    Returns:
      :class:`~apache_beam.transforms.ptransform.PTransform`
    """
    # TODO: Allow passing in your own sql statement
    if extra_client_params is None:
      extra_client_params = {}

    if ordering_col is not None:
      warnings.warn('You are using an ordering column to determine splits. This may'
                    'result in data being lost if writes happen during reads.', ResourceWarning)

    self.beam_options = {'user': user,
                         'password': password,
                         'host': host,
                         'database': database,
                         'table': table,
                         'columns': columns,
                         'port': port,
                         'extra_client_params': extra_client_params,
                         'ordering_col': ordering_col}
    self._mysql_source = _BoundedMysqlSource(**self.beam_options)

  def expand(self, pcoll):
    return pcoll | iobase.Read(self._mysql_source)


class _BoundedMysqlSource(iobase.BoundedSource):
  def __init__(self,
               user=None,
               password=None,
               host=None,
               database=None,
               table=None,
               columns=None,
               port=None,
               extra_client_params=None,
               ordering_col=None):
    if extra_client_params is None:
      extra_client_params = {}

    self.user = user
    self.password = password
    self.host = host
    self.database = database
    self.table = table
    self.port = port
    self.extra_client_params = extra_client_params
    self.ordering_col = ordering_col

    self._columns = columns
    self._primary_key = None
    self._mysql_version = None

  def estimate_size(self):  # type: () -> Optional[int]
    with pymysql.connect(user=self.user,
                         password=self.password,
                         host=self.host,
                         port=self.port,
                         database=self.database,
                         **self.extra_client_params,
    ) as cursor:
      sql = "SELECT COUNT(*) FROM {table}".format(table=self.table)
      cursor.execute(sql)
      result = cursor.fetchone()[0]
      return result

  def split(self,
            desired_bundle_size, # type: int
            start_position=None, # type: Optional[int]
            stop_position=None, # type: Optional[int]
            ):
    start_position, stop_position = self._replace_none_positions(
      start_position, stop_position)

    split_ranges = np.arange(start_position, stop_position, desired_bundle_size)

    bundle_start = start_position
    for split_range in split_ranges:
      if bundle_start >= stop_position:
        break
      bundle_end = min(stop_position, split_range)
      yield iobase.SourceBundle(weight=desired_bundle_size,
                                source=self,
                                start_position=bundle_start,
                                stop_position=bundle_end)
      bundle_start = bundle_end
    if bundle_start < stop_position:
      yield iobase.SourceBundle(weight=desired_bundle_size,
                                source=self,
                                start_position=bundle_start,
                                stop_position=stop_position)

  def get_range_tracker(self, start_position, stop_position):
    start_position, stop_position = self._replace_none_positions(
      start_position, stop_position)
    range_tracker = OrderedPositionRangeTracker(start_position, stop_position)
    if self.primary_key or self.ordering_col:
      return range_tracker
    return UnsplittableRangeTracker(range_tracker)

  def read(self,
           range_tracker, # type: OrderedPositionRangeTracker
           ):
    start = range_tracker.start_position()
    stop = range_tracker.stop_position()
    bundle_size = stop - start

    # Only version 8+ supports window functions, which are significiantly
    # faster than OFFSET for large query results
    columns = ', '.join(self.columns)
    if self.mysql_version >= 8:
      sql = """
      WITH cte AS
      (
        SELECT 
          *,
          ROW_NUMBER() OVER (ORDER BY {order_col}) AS row_num
        FROM {table}
      )
      SELECT {columns}
      FROM cte
      WHERE row_num >= {start}
      AND row_num <= {stop}
      """.format(table=self.table,
                 order_col=self.order_col,
                 columns=columns,
                 start=start,
                 stop=stop,
                 )
    else:
      sql = """
      SELECT {columns} 
      FROM {table}
      ORDER BY {order_col} ASC
      LIMIT {bundle_size}
      OFFSET {start}
      """.format(columns=columns,
                 table=self.table,
                 order_col=self.order_col,
                 bundle_size=bundle_size,
                 start=start,
                 )
    _LOGGER.debug('Reading using sql: %s', sql)
    with pymysql.connect(user=self.user,
                         password=self.password,
                         host=self.host,
                         port=self.port,
                         database=self.database,
                         cursorclass=pymysql.cursors.DictCursor,
                         **self.extra_client_params,
                         ) as cursor:
      cursor.execute(sql)
      result = cursor.fetchall()
      return result

  def display_data(self):
    res = super(_BoundedMysqlSource, self).display_data()
    res['user'] = self.user
    res['host'] = self.host
    res['port'] = self.port
    res['database'] = self.database
    res['table'] = self.table
    res['extra_client_params'] = json.dumps(self.extra_client_params)
    res['ordering_col'] = self.ordering_col
    return res

  @property
  def columns(self):
    if self._columns is None:
      sql = """
      SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
      WHERE `TABLE_SCHEMA` = '{database}'
      AND `TABLE_NAME` = '{table}'
      ORDER BY `TABLE_NAME`, `ORDINAL_POSITION`
      """.format(
        database=self.database, table=self.table)
      with pymysql.connect(user=self.user,
                           password=self.password,
                           host=self.host,
                           port=self.port,
                           database=self.database,
                           **self.extra_client_params,
                           ) as cursor:
        cursor.execute(sql)
        columns = [col[0] for col in cursor.fetchall()]
        self._columns = columns
        _LOGGER.debug('Fetched columns %s', self._columns)
    return self._columns


  @property
  def primary_key(self):
    if self._primary_key is None:
      sql = """
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = SCHEMA()
       AND TABLE_NAME = '{table}'
       AND COLUMN_KEY = 'PRI'
      """.format(table=self.table)
      _LOGGER.debug('Finding primary key using sql: %s', sql)

      with pymysql.connect(user=self.user,
                           password=self.password,
                           host=self.host,
                           port=self.port,
                           database=self.database,
                           **self.extra_client_params,
                           ) as cursor:
        cursor.execute(sql)
        self._primary_key = cursor.fetchone()[0]
        _LOGGER.debug('Primary key for %s is %s', self.table, self.primary_key)
    return self._primary_key

  @property
  def order_col(self):
    if self.ordering_col is not None:
      return self.ordering_col
    return self.primary_key

  @property
  def mysql_version(self):
    if self._mysql_version is None:
      sql = 'SELECT VERSION()'
      with pymysql.connect(user=self.user,
                           password=self.password,
                           host=self.host,
                           port=self.port,
                           database=self.database,
                           **self.extra_client_params,
                           ) as cursor:
        cursor.execute(sql)
        mysql_version = cursor.fetchone()[0]
        _LOGGER.debug('Using MySQL version %s', mysql_version)
        self._mysql_version = int(mysql_version[0]) # Round down
    return self._mysql_version

  def _replace_none_positions(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      size = self.estimate_size()
      # increment last value by 1 to make sure the last row
      # is not excluded
      stop_position = size + 1
    return start_position, stop_position


@experimental()
class WriteToMysql(PTransform):
  """ A transform to write to the MySql Table.
  A PTransform that write a list of `DirectRow` into the Mysql Table
  """
  def __init__(self,
               user=None,
               password=None,
               host=None,
               database=None,
               table=None,
               port=None,
               batch_size=100,
               extra_client_params=None):
    """ Constructor of the Write connector of MySQL
    Args:
      user(str): MySQL user to connect with
      password(str): MySQL password to connect with
      host(str): MySQL host to connect to
      database(str): Name of the database on the MySQL database server to connect to
      table_(str): MySQL Table to write the `DirectRows`
      port(int): Optional MySQL port to connect to, defaults to standard MySQL port 3306
      batch_size(int): Number of rows per bulk_write to write to MySQL, default to 100
      extra_client_params(dict): Optional `pymysql.connect
        https://pymysql.readthedocs.io/en/latest/modules/connections.html` parameters as
        keyword arguments
    """
    if extra_client_params is None:
      extra_client_params = {}

    self.beam_options = {'user': user,
                         'password': password,
                         'host': host,
                         'database': database,
                         'table': table,
                         'port': port,
                         'batch_size': batch_size,
                         'extra_client_params': extra_client_params}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | beam.ParDo(_MysqlWriteFn(**beam_options))
            )


class _MysqlWriteFn(DoFn):
  """ Creates the connector can call and add_row to the batcher using each
  row in beam pipe line
  Args:
    user(str): MySQL user to connect with
    password(str): MySQL password to connect with
    host(str): MySQL host to connect to
    port(str): MySQL port to connect to
    database(str): Name of the database on the MySQL database server to connect to
    table(str): MySQL Table to write the `DirectRows`
    batch_size(int): Number of rows per bulk_write to write to MySQL
    extra_client_params(dict): `pymysql.connect
        https://pymysql.readthedocs.io/en/latest/modules/connections.html` parameters as
        keyword arguments
  """

  def __init__(self,
               user, # type: str
               password, # type: str
               host, # type: str
               port, # type: int
               database, # type: str
               table, # type: str
               batch_size, # type: Optional[int]
               extra_client_params # type: Optional[Dict]
               ):
    """Constructor of the Write connector of MySQL
    Args:
      user(str): MySQL user to connect with
      password(str): MySQL password to connect with
      host(str): MySQL host to connect to
      port(str): MySQL port to connect to
      database(str): Name of the database on the MySQL database server to connect to
      table_(str): MySQL Table to write the `DirectRows`
      batch_size(int): Number of rows per bulk_write to write to MySQL
      extra_client_params(dict): `pymysql.connect
        https://pymysql.readthedocs.io/en/latest/modules/connections.html` parameters as
        keyword arguments
    """
    super(_MysqlWriteFn, self).__init__()
    self.user = user
    self.password = password
    self.host = host
    self.port = port
    self.database = database
    self.table = table
    self.batch_size = batch_size
    self.extra_client_params = extra_client_params

    self.cursor = None
    self.batch = []
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def process(self, element):
    self.batch.append(element)
    if len(self.batch) >= self.batch_size:
      self._flush()

  def finish_bundle(self):
    self._flush()

  def _flush(self):
    if len(self.batch) == 0:
      return

    with _MySQLSink(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            table=self.table,
            **self.extra_client_params,
    ) as sink:
      sink.write(self.batch)
      self.batch = []

  def display_data(self):
    res = super(_MysqlWriteFn, self).display_data()
    res['user'] = self.user
    res['host'] = self.host
    res['port'] = self.port
    res['database'] = self.database
    res['table'] = self.table
    res['extra_client_params'] = json.dumps(self.extra_client_params)
    res['batch_size'] = self.batch_size
    return res


class _MySQLSink(object):
  def __init__(self,
               user, # type: str
               password, # type: str
               host, # type: str
               port, # type: int
               database, # type: str
               table, # type: str
               extra_client_params=None,  # type: Optional[Dict]
               ):
    if extra_client_params is None:
      extra_client_params = {}
    self.user = user
    self.password = password
    self.host = host
    self.port = port
    self.database = database
    self.table = table
    self.extra_client_params = extra_client_params

    self.connection = None
    self.columns = None
    self._upsert_stmt = None

  def write(self, batch):
    _LOGGER.debug('Batch to insert %s', batch)
    if self.connection is None:
      self.connection = pymysql.connect(
        user=self.user,
        password=self.password,
        host=self.host,
        port=self.port,
        database=self.database,
        **self.extra_client_params,
      )
    with self.connection.cursor() as cursor:
      if self.columns is None:
        self._get_columns(cursor)

      cursor.executemany(self.upsert_stmt, batch)

    # TODO: Write better debug message and retry writes
    _LOGGER.debug('BulkWrite to MySQL successful')

  @property
  def upsert_stmt(self):
    # TODO: include pk column in upsert
    if self._upsert_stmt is None:
      columns_fmt = ', '.join(self.columns)
      values_fmt = ', '.join(['%(' + col + ')s' for col in self.columns])
      update_fmt = ', '.join(['`%(col)s`=VALUES(`%(col)s`)' % {'col': col} for col in self.columns])
      upsert_stmt = """
      INSERT INTO {table}
      ({columns_fmt})
      VALUES ({values_fmt})
      ON DUPLICATE KEY UPDATE {update_fmt}
      """.format(table=self.table,
                 columns_fmt=columns_fmt,
                 values_fmt=values_fmt,
                 update_fmt=update_fmt)
      _LOGGER.debug('Prepared upsert statement %s', upsert_stmt)
      self._upsert_stmt = upsert_stmt
    return self._upsert_stmt

  def _get_columns(self,
                   cursor # type: pymysql.connections.Cursor,
                   ):
    sql = """
      SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
      WHERE `TABLE_SCHEMA` = '{database}'
      AND `TABLE_NAME` = '{table}'
      AND `EXTRA` NOT LIKE '%auto_increment%'
      ORDER BY `TABLE_NAME`, `ORDINAL_POSITION`
      """.format(
      database=self.database, table=self.table)
    # This ignores auto incrementing columns as they will not be included in
    # the upsert statement
    _LOGGER.debug('Executing SQL statement to get columns: %s', sql)
    cursor.execute(sql)
    columns = [col[0] for col in cursor.fetchall()]
    self.columns = columns
    _LOGGER.debug('Using columns %s', self.columns)

  def __enter__(self):
    if self.connection is None:
      self.connection = pymysql.connect(
        user=self.user,
        password=self.password,
        host=self.host,
        port=self.port,
        database=self.database,
        **self.extra_client_params,
      )
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self.connection is not None:
      self.connection.commit()
      self.connection.close()
