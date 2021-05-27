#!/usr/bin/env python3
from io import StringIO
import pandas as pd
import traceback
import psycopg2
import boto3
import sys
import os
import re
import uuid
import logging

S3_ACCEPTED_KWARGS = [
    'ACL', 'Body', 'CacheControl ',  'ContentDisposition', 'ContentEncoding', 'ContentLanguage',
    'ContentLength', 'ContentMD5', 'ContentType', 'Expires', 'GrantFullControl', 'GrantRead',
    'GrantReadACP', 'GrantWriteACP', 'Metadata', 'ServerSideEncryption', 'StorageClass',
    'WebsiteRedirectLocation', 'SSECustomerAlgorithm', 'SSECustomerKey', 'SSECustomerKeyMD5',
    'SSEKMSKeyId', 'RequestPayer', 'Tagging'
]  # Available parameters for service: https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.put_object

logging_config = {
    'logger_level': logging.INFO,
    'mask_secrets': True
}
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def set_log_level(level, mask_secrets=True):
    log_level_map = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warn': logging.WARN,
        'error': logging.ERROR
    }
    logging_config['logger_level'] = log_level_map[level]
    logger = logging.getLogger(__name__)
    logger.setLevel(logging_config['logger_level'])
    logging_config['mask_secrets'] = mask_secrets

def mask_aws_credentials(s):
    if logging_config['mask_secrets']:
        import re
        s = re.sub('(?<=access_key_id \')(.*)(?=\')', '*'*8, s)
        s = re.sub('(?<=secret_access_key \')(.*)(?=\')', '*'*8, s)
    return s

##############
#config types#
##############
from dataclasses import dataclass,asdict
@dataclass
class RedshiftConfig:
    database: str
    schema: str 
    host: str
    user: str
    password: str
    port: int = 5439

@dataclass
class S3Config:
    access_key: str
    secret_access_key: str 
    bucket: str
    subdirectory: str = None
    iam_role: str = None
    token: str = None

class RedPanda:
    def __init__(self,redshiftconfig,s3config=None):
        self.redshiftconf = redshiftconfig
        self.s3conf = s3config
        
        self._connect_to_redshift()
        if s3config is not None:
            if self.s3conf.subdirectory is None:
                self.s3conf.subdirectory = ''
            else:
                self.s3conf.subdirectory += '/'
            self._connect_to_s3()

    def _connect_to_redshift(self, **kwargs):
        cfg = self.redshiftconf
        self.connect = psycopg2.connect(dbname=cfg.database,
                                   host=cfg.host,
                                   port=cfg.port,
                                   user=cfg.user,
                                   password=cfg.password,
                                   **kwargs)
        self.cursor = self.connect.cursor()

    def _connect_to_s3(self,**kwargs):
        assert (self.s3conf is not None),"No s3 config provided"
        self.s3 = boto3.resource('s3',
                            aws_access_key_id=self.s3conf.access_key,
                            aws_secret_access_key=self.s3conf.secret_access_key,
                            **kwargs)

    def query(self,sql_query, query_params=None):
        # pass a sql query and return a pandas dataframe
        self.cursor.execute(sql_query, query_params)
        columns_list = [desc[0] for desc in self.cursor.description]
        data = pd.DataFrame(self.cursor.fetchall(), columns=columns_list)
        return data

    def validate_column_names(self,data_frame):
        """Validate the column names to ensure no reserved words are used.

        Arguments:
            dataframe pd.data_frame -- data to validate
        """
        rrwords = open(os.path.join(os.path.dirname(__file__),
                                    'redshift_reserve_words.txt'), 'r').readlines()
        rrwords = [r.strip().lower() for r in rrwords]

        data_frame.columns = [x.lower() for x in data_frame.columns]

        for col in data_frame.columns:
            try:
                assert col not in rrwords
            except AssertionError:
                raise ValueError(
                    'DataFrame column name {0} is a reserve word in redshift'
                    .format(col))

        # check for spaces in the column names
        there_are_spaces = sum(
            [re.search('\s', x) is not None for x in data_frame.columns]) > 0
        # delimit them if there are
        if there_are_spaces:
            col_names_dict = {x: '"{0}"'.format(x) for x in data_frame.columns}
            data_frame.rename(columns=col_names_dict, inplace=True)
        return data_frame

    def df_to_s3(self,data_frame, csv_name, index, save_local, delimiter, verbose=True, **kwargs):
        """Write a dataframe to S3

        Arguments:
            dataframe pd.data_frame -- data to upload
            csv_name str -- name of the file to upload
            save_local bool -- save a local copy
            delimiter str -- delimiter for csv file
        """
        extra_kwargs = {k: v for k, v in kwargs.items(
        ) if k in S3_ACCEPTED_KWARGS and v is not None}
        # create local backup
        if save_local:
            data_frame.to_csv(csv_name, index=index, sep=delimiter)
            if verbose:
                logger.info(f'saved file {csv_name} in {os.getcwd}')
        #
        csv_buffer = StringIO()
        data_frame.to_csv(csv_buffer, index=index, sep=delimiter)
        self.s3.Bucket(self.s3conf.bucket).put_object(
            Key=self.s3conf.subdirectory + csv_name, Body=csv_buffer.getvalue(),
            **extra_kwargs)
        if verbose:
            logger.info(f'saved file {csv_name} in bucket {self.s3conf.subdirectory + csv_name}')

    def pd_dtype_to_redshift_dtype(self,dtype):
        if dtype.startswith('int64'):
            return 'BIGINT'
        elif dtype.startswith('int'):
            return 'INTEGER'
        elif dtype.startswith('float'):
            return 'REAL'
        elif dtype.startswith('datetime'):
            return 'TIMESTAMP'
        elif dtype == 'bool':
            return 'BOOLEAN'
        else:
            return 'VARCHAR(256)'

    def get_column_data_types(self,data_frame, index=False):
        column_data_types = [self.pd_dtype_to_redshift_dtype(dtype.name)
                             for dtype in data_frame.dtypes.values]
        if index:
            column_data_types.insert(
                0, self.pd_dtype_to_redshift_dtype(data_frame.index.dtype.name))
        return column_data_types

    def create_redshift_table(self,
                              data_frame,
                              redshift_table_name,
                              column_data_types=None,
                              index=False,
                              append=False,
                              diststyle='even',
                              distkey='',
                              sort_interleaved=False,
                              sortkey='',
                              verbose=True):
        """Create an empty RedShift Table

        """
        if index:
            columns = list(data_frame.columns)
            if data_frame.index.name:
                columns.insert(0, data_frame.index.name)
            else:
                columns.insert(0, "index")
        else:
            columns = list(data_frame.columns)
        if column_data_types is None:
            column_data_types = self.get_column_data_types(data_frame, index)
        columns_and_data_type = ', '.join(
            ['{0} {1}'.format(x, y) for x, y in zip(columns, column_data_types)])

        create_table_query = 'create table {0} ({1})'.format(
            redshift_table_name, columns_and_data_type)
        if not distkey:
            # Without a distkey, we can set a diststyle
            if diststyle not in ['even', 'all']:
                raise ValueError("diststyle must be either 'even' or 'all'")
            else:
                create_table_query += ' diststyle {0}'.format(diststyle)
        else:
            # otherwise, override diststyle with distkey
            create_table_query += ' distkey({0})'.format(distkey)
        if len(sortkey) > 0:
            if sort_interleaved:
                create_table_query += ' interleaved'
            create_table_query += ' sortkey({0})'.format(sortkey)
        if verbose:
            logger.info(create_table_query)
            logger.info('CREATING A TABLE IN REDSHIFT')
        self.cursor.execute('drop table if exists {0}'.format(redshift_table_name))
        self.cursor.execute(create_table_query)
        self.connect.commit()

    def s3_to_redshift(self,redshift_table_name, csv_name, delimiter=',', quotechar='"',
                       dateformat='auto', timeformat='auto', region='', parameters='', verbose=True):

        bucket_name = 's3://{0}/{1}'.format(
            self.s3conf.bucket, self.s3conf.subdirectory + csv_name)

        aws1 = self.s3conf.access_key
        aws2 = self.s3conf.secret_access_key
        aws_role = self.s3conf.iam_role
        if aws1 and aws2:
            authorization = f"""
            access_key_id '{aws1}'
            secret_access_key '{aws2}'
            """
        elif aws_role:
            authorization = f"""
            iam_role '{aws_role}'
            """
        else:
            authorization = ""

        s3_to_sql = f"""
        copy {redshift_table_name}
        from '{bucket_name}'
        delimiter '{delimiter}'
        ignoreheader 1
        csv quote as '{quotechar}'
        dateformat '{dateformat}'
        timeformat '{timeformat}'
        {authorization}
        {parameters}
        """
        if region:
            s3_to_sql += f"region '{region}'"
        if self.s3conf.token:
            s3_to_sql += f"\n\tsession_token '{self.s3conf.token}'"
        s3_to_sql = s3_to_sql + ';'
        if verbose:
            logger.info(mask_aws_credentials(s3_to_sql))
            # send the file
            logger.info('FILLING THE TABLE IN REDSHIFT')
        try:
            self.cursor.execute(s3_to_sql)
            self.connect.commit()
        except Exception as e:
            logger.error(e)
            traceback.print_exc(file=sys.stdout)
            self.connect.rollback()
            raise

    def exists(self,table):
        query = (f"SELECT EXISTS ("
                 f"SELECT * FROM information_schema.tables "
                 f"WHERE  table_schema = '{self.redshiftconf.schema}'"
                 f"AND    table_name   = '{table.lower()}');")
        self.cursor.execute(query)
        return self.cursor.fetchall()[0][0]

    def pandas_to_redshift(self,
                           data_frame,
                           redshift_table_name,
                           column_data_types=None,
                           index=False,
                           save_local=False,
                           delimiter=',',
                           quotechar='"',
                           dateformat='auto',
                           timeformat='auto',
                           region='',
                           append=False,
                           diststyle='even',
                           distkey='',
                           sort_interleaved=False,
                           sortkey='',
                           parameters='',
                           verbose=True,
                           overwritre=False,
                           **kwargs):
        
        # Validate column names.
        data_frame = self.validate_column_names(data_frame)
        # Send data to S3
        csv_name = '{}-{}.csv'.format(redshift_table_name, uuid.uuid4())
        s3_kwargs = {k: v for k, v in kwargs.items()
            if k in S3_ACCEPTED_KWARGS and v is not None}
        self.df_to_s3(data_frame, csv_name, index, save_local, delimiter, verbose=verbose, **s3_kwargs)

        # CREATE AN EMPTY TABLE IN REDSHIFT
        if not append:
            self.create_redshift_table(data_frame, redshift_table_name,
                                  column_data_types, index, append,
                                  diststyle, distkey, sort_interleaved, sortkey, verbose=verbose)

        # CREATE THE COPY STATEMENT TO SEND FROM S3 TO THE TABLE IN REDSHIFT
        self.s3_to_redshift(redshift_table_name, csv_name, delimiter, quotechar,
                       dateformat, timeformat, region, parameters, verbose=verbose)
        
    def put(self,df,table,append=False):
        self.pandas_to_redshift(df,self.redshiftconf.schema+'.'+table,append=append)
        
    def load(self,table):
        return self.query(f'select * from {self.redshiftconf.schema}.{table}')
        
    def close(self):
        self.cursor.close()
        self.connect.commit()

    def __enter__(self):
        return self
    def __exit__(self,exc_type, exc_value, tb):
        self.close()
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            return False

        return True