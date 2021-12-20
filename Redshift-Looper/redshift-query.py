import boto3
import os
import pandas as pd
import time
from datetime import datetime
from io import StringIO
from interfaces.aws.s3 import S3
from datahub_logger import getLogger
LOGGER = getLogger()


def lambda_handler(event, context):
    LOGGER.lambda_init(event, context)
    db_users_groups = SubtenantRedshiftDB()
    db_users_groups.execute()


class SubtenantRedshiftDB:
    def __init__(self):
        self._account_name = os.environ.get('ACCOUNT_NAME')
        self._tenant_id = os.environ.get('TENANT_ID')
        self._tenant_env = os.environ.get('TENANT_ENV')
        self._internal_bucket_name = f"{self._account_name}-" \
                                     f"{self._tenant_id}-{self._tenant_env}" \
                                     f"-internal"
        self._redshift_client = boto3.client('redshift-data',
                                             region_name='eu-west-1')
        self._current_time = f"ds={datetime.utcnow().strftime('%Y%m%d')}"
        self._prefix = "services/access_report/redshift"
        self._filename = "redshift_users_groups-" \
                         f"{os.environ.get('ACCOUNT_NAME')}-" \
                         f"{os.environ.get('TENANT_ID')}-" \
                         f"{os.environ.get('TENANT_ENV')}.csv"
        self._key = f"{self._prefix}{self._current_time}/{self._filename}"
        self._server_side_encryption = "AES256"
        self._cluster_identifier = ["", ""]
        self._database = ""
        self._dbuser = ""
        self._filename = "redshift_users_groups.csv"
        self._sqlalldatabase = '''\
                    SELECT datname from pg_database;\
                    '''
        self._sql = '''\
                    Change this as it fits \
                    SELECT current_database() as database_name,\
                    usename as user_name,\
                    groname as grp_name\
                    FROM pg_user, pg_group\
                    WHERE pg_user.usesysid = ANY(pg_group.grolist)\
                    AND pg_group.groname in\
                    (SELECT DISTINCT pg_group.groname from pg_group);\
                    '''

    def execute(self):
        df_cluster = []
        for c_idf in self._cluster_identifier:
            allpermission = []
            all_databases = self._get_redshift_lists_all_databases(c_idf)
            for i in all_databases['Records']:
                for key, all_databases in i[0].items():
                    if all_databases.endswith('db'):
                        if all_databases != 'maindb':
                            data = self._get_redshift_users_groups(
                                   all_databases, c_idf
                                   )
                            allpermission.append(data)
            # print(f"cluster", i , "number of databases", len(allpermission))
            dfnest = self._redshift_to_dataframe(allpermission, c_idf)
            df_cluster.append(dfnest)
        dfnest = pd.concat(df_cluster)
        self._write_csv(dfnest)

    def _get_redshift_lists_all_databases(self, cluster_identifier):
        LOGGER.info(f"Obtaining users and groups from "
                    f"Database: {self._database} "
                    f"in Redshift cluster: {cluster_identifier}")
        response = self._redshift_client.execute_statement(
            ClusterIdentifier=cluster_identifier,
            Database=self._database,
            DbUser=self._dbuser,
            Sql=self._sqlalldatabase
        )
        LOGGER.info("Obtaining response")
        query_id = response['Id']
        LOGGER.info("Obtaining query_id")
        print(query_id)
        while self._redshift_client.describe_statement(
            Id=query_id
                )['Status'] != "FINISHED":
            print("**** waiting 5 sec for FINISHED status ****")
            time.sleep(5)
        LOGGER.info("checking query_id is FINISHED")
        LOGGER.info("printing query_id")
        print(self._redshift_client.describe_statement(Id=query_id)['Status'])
        data = self._redshift_client.get_statement_result(Id=query_id)
        return data

    def _get_redshift_users_groups(self, database, cluster_identifier):
        LOGGER.info(f"Obtaining users and groups from "
                    f"Database: {database} "
                    f"in Redshift cluster: {cluster_identifier}")
        response = self._redshift_client.execute_statement(
            ClusterIdentifier=cluster_identifier,
            Database=database,
            DbUser=self._dbuser,
            Sql=self._sql
        )
        LOGGER.info("Obtaining response")
        query_id = response['Id']
        LOGGER.info("Obtaining query_id")
        print(query_id)
        while self._redshift_client.describe_statement(
            Id=query_id
                )['Status'] != "FINISHED":
            print("**** waiting 5 sec for FINISHED status ****")
            time.sleep(5)
        LOGGER.info("checking query_id is FINISHED")
        LOGGER.info("printing query_id")
        print(self._redshift_client.describe_statement(Id=query_id)['Status'])
        data = self._redshift_client.get_statement_result(Id=query_id)

        # print(f"" , database , "" , data )
        # print(json.dumps(data, indent = 1, sort_keys=True))
        return data

    def _redshift_to_dataframe(self, data, cluster_identifier):
        LOGGER.info("Obtaining nested attributes from "
                    f"Database table: {self._database}")

        label = 0
        df_labels = []

        for per_database in data:
            while label == 0:
                for i in per_database['ColumnMetadata']:
                    df_labels.append(i['label'])
                    label = 1
                print(df_labels)

        dfnest = pd.DataFrame(columns=df_labels)

        for per_database in data:
            df_data = []
            for i in per_database['Records']:
                object_data = []
                for j in i:
                    object_data.append(list(j.values())[0])

                df_data.append(object_data)
                # print(object_data)
                # print(f"df", df_data)
            dfperdatabase = pd.DataFrame(columns=df_labels, data=df_data)
            dfnest = dfnest.append(dfperdatabase)

        # print(dfnest)
        dfnest.insert(0, 'account_name', self._account_name)
        dfnest.insert(1, 'tenant_id', self._tenant_id)
        dfnest.insert(2, 'tenant_env', self._tenant_env)
        dfnest.insert(3, 'cluster_name', cluster_identifier)
        return dfnest

    def _write_csv(self, dfnest):
        df = pd.DataFrame(dfnest)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, sep=",", index=False)
        LOGGER.info(f"writing CSV file to {self._internal_bucket_name} with "
                    f"key {self._key}")
        s3 = S3()
        s3.put_object(
                    bucket_name=self._internal_bucket_name,
                    obj_key=self._key,
                    body=csv_buffer.getvalue(),
                    sse=self._server_side_encryption)
        LOGGER.info("uploaded file")
