from dbt.cli.main import dbtRunner, dbtRunnerResult
import argparse
from utils import get_conn_string, get_target_schema
from sqlalchemy import create_engine, text
import pandas as pd

def rundbt(cli_args):
    dbt = dbtRunner()
    res = dbt.invoke(cli_args)
    things_created = []
    for r in res.result:
        print(f"{r.node.name}: {r.status}")
        things_created.append(r.node.name)
    return things_created

if (__name__ == '__main__'):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='ammi', help='dbt project name, default to ammi here')
    parser.add_argument('--target', default='dev', help='dbt target name, default to dev')
    parser.add_argument('--model', help='model name to be used in dbt select', required=True)
    parser.add_argument('--report', help='report to be used in dbt vars', default='')
    parser.add_argument('--profile_path', help='path to the profile yaml file', default='C:/Users/zhma/.dbt/profiles.yml')
    parser.add_argument('--output_parquet', help='output to a parquet file (no output to parquet if none)', default='')
    args = parser.parse_args()
    print(args)

    # first do the seed as I need to do drop table
    cli_args = ['seed', '--full-refresh', '--target', args.target]
    tables_todrop = rundbt(cli_args)
    print(tables_todrop)

    # then create the model itself
    cli_args = ["run", "--select", f"+{args.model}", 
                "--full-refresh", '--target', args.target]
    if len(args.report) > 0:
        cli_args.extend(["--vars", f"{{'report': '{args.report}'}}"]) 
    views_todrop = rundbt(cli_args)
    print(views_todrop)

    # prepare the connection string
    conn_string = get_conn_string(args.profile_path, args.project, args.target)
    # print(conn_string)
    schema = get_target_schema(args.profile_path, args.project, args.target)
    engine = create_engine(conn_string)

    # if it needs to output to a parquet file
    if len(args.output_parquet) > 0:
        stmt = f'select * from {schema}.{args.model}'
        dat = pd.read_sql(stmt, con=engine)
        dat.to_parquet(args.output_parquet)

    # delete the intermediate views and seed tables
    with engine.connect() as conn:

        # drop seed tables
        for table in tables_todrop:
            stmt = f'drop table {schema}.{table}'
            conn.execute(text(stmt))
            conn.commit()

        # drop intermediate views
        for view in views_todrop:
            if view == args.model:  continue
            stmt = f'drop view {schema}.{view}'
            conn.execute(text(stmt))
            conn.commit()
    
