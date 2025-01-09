import yaml

def get_conn_string(yaml_path, project, target):
    with open(yaml_path, 'r') as fid:
        yaml_dict = yaml.safe_load(fid)
    profile = yaml_dict[project]['outputs'][target]
    return f"mssql+pyodbc://@{profile['server']}/{profile['database']}?driver=ODBC+Driver+17+for+SQL+Server"

def get_target_schema(yaml_path, project, target):
    with open(yaml_path, 'r') as fid:
        yaml_dict = yaml.safe_load(fid)
    profile = yaml_dict[project]['outputs'][target]
    return profile['schema']

    