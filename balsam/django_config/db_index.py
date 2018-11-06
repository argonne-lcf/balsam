import json
import os

def refresh_db_index():
    home_dir = os.path.expanduser('~')
    BALSAM_HOME = os.path.join(home_dir, '.balsam')

    index_path = os.path.join(BALSAM_HOME, 'databases.json')

    if os.path.exists(index_path):
        with open(index_path) as fp: db_list = json.load(fp)
    else:
        db_list = []
        if not os.path.exists(BALSAM_HOME):
            os.makedirs(BALSAM_HOME)

    cur_db = os.environ.get('BALSAM_DB_PATH')
    if cur_db:
        cur_db = os.path.abspath(os.path.expanduser(cur_db))
        if cur_db not in db_list: db_list.append(cur_db)

    for i, db in reversed(list(enumerate(db_list[:]))):
        if not os.path.exists(db): del db_list[i]
    with open(index_path, 'w') as fp: json.dump(db_list, fp, indent=1)
    return db_list
