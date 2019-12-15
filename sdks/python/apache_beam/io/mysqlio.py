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
                           batch_size=1000)


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

import pymysql

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.utils.annotations import experimental

_LOGGER = logging.getLogger(__name__)


@experimental()
class WriteToMysql(PTransform):
  """ A transform to write to the MySql Table.
  A PTransform that write a list of `DirectRow` into the Mysql Table
  """
  def __init__(self, user, password, host, database, table, port=3306, batch_size=100, extra_client_params=None):
    """ Constructor of the Write connector of MySQL
    Args:
      user(str): MySQL user to connect with
      password(str): MySQL password to connect with
      host(str): MySQL host to connect to
      database(str): Name of the database on the MySQL database server to connect to
      table_(str): MySQL Table to write the `DirectRows`
      port(int): Optional MySQL port to connect to, defaults to standard MySQL port 3306
      batch_size(int): Number of rows per bulk_write to write to MySQL, default to 100
      extra_client_params(dict): Optional `pymysql.connections.Connection
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
    extra_client_params(dict): `pymysql.connections.Connection
        https://pymysql.readthedocs.io/en/latest/modules/connections.html` parameters as
        keyword arguments
  """

  def __init__(self, user, password, host, port, database, table, batch_size, extra_client_params):
    """Constructor of the Write connector of MySQL
    Args:
      user(str): MySQL user to connect with
      password(str): MySQL password to connect with
      host(str): MySQL host to connect to
      port(str): MySQL port to connect to
      database(str): Name of the database on the MySQL database server to connect to
      table_(str): MySQL Table to write the `DirectRows`
      batch_size(int): Number of rows per bulk_write to write to MySQL
      extra_client_params(dict): `pymysql.connections.Connection
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
  def __init__(self, user, password, host, port, database, table, extra_client_params=None):
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
    self._insert_stmt = None
    self._columns_fmt = None
    self._values_fmt = None

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

      cursor.executemany(self.insert_stmt, batch)
      self.connection.commit()

    # TODO: Write better debug message and retry writes
    _LOGGER.debug('BulkWrite to MySQL successful')

  @property
  def insert_stmt(self):
    if self._insert_stmt is None:
      # TODO: Figure out how to prepare this statement or analyze it for SQL injection
      insert_stmt = """
      INSERT INTO {table}
      ({columns_fmt})
      VALUES ({values_fmt})
      """.format(table=self.table,
                 columns_fmt=self.columns_fmt,
                 values_fmt=self.values_fmt)
      _LOGGER.debug('Prepared insert statement %s', insert_stmt)
      self._insert_stmt = insert_stmt
    return self._insert_stmt

  @property
  def columns_fmt(self):
    if self._columns_fmt is None:
      self._column_fmt = ', '.join(self.columns)
    return self._column_fmt

  @property
  def values_fmt(self):
    if self._values_fmt is None:
      value_fmt = ', '.join(['%(' + col + ')s' for col in self.columns])
      self._values_fmt = value_fmt
    return self._values_fmt

  def _get_columns(self,
                   cursor # type: pymysql.connections.Cursor,
                   ):
    sql = """
            SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
            WHERE `TABLE_SCHEMA` = '{database}'
            AND `TABLE_NAME` = '{table}'
            ORDER BY `TABLE_NAME`, `ORDINAL_POSITION`""".format(
      database=self.database, table=self.table)
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
      self.connection.close()
