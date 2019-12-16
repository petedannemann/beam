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

from __future__ import absolute_import

import argparse
import logging
import time

import pymysql

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)


def setup_mysql(user, password, host, database, table, port):
  sql = """
  CREATE DATABASE IF NOT EXISTS {database};
  USE {database};
  DROP TABLE IF EXISTS `{table}`;
  CREATE TABLE `{table}` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    number INT,
    number_mod_2 INT,
    number_mod_3 INT
  );
  """.format(database=database,
             table=table)

  try:
    conn = pymysql.connect(
      user=user,
      password=password,
      host=host,
      port=port,
      client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS)
    with conn.cursor() as cursor:
      cursor.execute(sql)
    conn.commit()
  finally:
    conn.close()


def teardown_mysql(user, password, host, database, port):
  with pymysql.connect(user=user,
                       password=password,
                       host=host,
                       port=port,
                       ) as cursor:
    sql = 'DROP DATABASE {database}'.format(database=database)
    cursor.execute(sql)


class GenerateRows(beam.DoFn):
  def process(self, num_rows, *args, **kwargs):
    for i in range(num_rows):
      yield {
          'number': i,
          'number_mod_2': i % 2,
          'number_mod_3': i % 3
      }


def run(argv=None):
  default_db = 'beam_mysqlio_it_db'
  default_table = 'integration_test'
  parser = argparse.ArgumentParser()
  parser.add_argument('--mysql_user',
                      default='user',
                      help='MySQL user to connect with')
  parser.add_argument('--mysql_password',
                      help='MySQL password to connect with')
  parser.add_argument('--mysql_host',
                      default='localhost',
                      help='MySQL host to connect to')
  parser.add_argument('--mysql_database',
                      default=default_db,
                      help='Name of the database on the MySQL database server to connect to')
  parser.add_argument('--table',
                      default=default_table,
                      help='MySQL Table to write to `DirectRows`')
  parser.add_argument('--mysql_port',
                      default=3306,
                      type=int,
                      help='Optional MySQL port to connect to, defaults to standard MySQL port 3306')
  parser.add_argument('--num_rows',
                      default=100000,
                      help='The expected number of rows to be generated '
                           'for write or read',
                      type=int)
  parser.add_argument('--batch_size',
                      default=10000,
                      type=int,
                      help=('Batch size for writing to MySQL'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  setup_mysql(known_args.mysql_user,
              known_args.mysql_password,
              known_args.mysql_host,
              known_args.mysql_database,
              known_args.table,
              known_args.mysql_port,
              )

  # Test Write to MySQL
  with TestPipeline(options=PipelineOptions(pipeline_args)) as p:
    start_time = time.time()
    _LOGGER.info('Writing %d rows to MySQL' % known_args.num_rows)

    _ = (p | beam.Create([known_args.num_rows])
         | 'Create rows' >> beam.ParDo(GenerateRows())
         | 'WriteToMySQL' >> beam.io.WriteToMysql(known_args.mysql_user,
                                                  known_args.mysql_password,
                                                  known_args.mysql_host,
                                                  known_args.mysql_database,
                                                  known_args.table,
                                                  known_args.mysql_port,
                                                  known_args.batch_size,
                                                  ))
  elapsed = time.time() - start_time
  _LOGGER.info('Writing %d rows to MySQL finished in %.3f seconds' %
               (known_args.num_rows, elapsed))

  # Test Read from MySQL
  # with TestPipeline(options=PipelineOptions(pipeline_args)) as p:
  #   start_time = time.time()
  #   _LOGGER.info('Reading from MySQL %s:%s' %
  #                (known_args.mysql_database, known_args.table))
  #   r = (p | 'ReadFromMysql' >> beam.io.ReadFromMysql((known_args.mysql_user,
  #                                                      known_args.mysql_password,
  #                                                      known_args.mysql_host,
  #                                                      known_args.mysql_database,
  #                                                      known_args.table,
  #                                                      known_args.mysql_port,
  #                                                      known_args.batch_size,
  #                                                      ))
  #          | 'Map' >> beam.Map(lambda doc: doc['number'])
  #          | 'Combine' >> beam.CombineGlobally(sum))
  #   assert_that(
  #       r, equal_to([sum(range(known_args.num_rows))]))
  #
  # elapsed = time.time() - start_time
  # _LOGGER.info('Read %d rows from MySQL finished in %.3f seconds' %
  #              (known_args.num_rows, elapsed))

  teardown_mysql(user=known_args.mysql_user,
                 password=known_args.mysql_password,
                 host=known_args.mysql_host,
                 database=known_args.mysql_database,
                 port=known_args.mysql_port,
                 )


if __name__ == "__main__":
  # TODO: Turn this back to INFO level
  logging.getLogger().setLevel(logging.DEBUG)
  run()
