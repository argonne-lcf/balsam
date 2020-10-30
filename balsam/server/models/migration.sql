BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 745d0fe434fc

CREATE TABLE users (
    id SERIAL NOT NULL, 
    username VARCHAR(100), 
    hashed_password VARCHAR(128), 
    PRIMARY KEY (id), 
    UNIQUE (username)
);

CREATE TABLE sites (
    id SERIAL NOT NULL, 
    hostname VARCHAR(100), 
    path VARCHAR(100), 
    last_refresh TIMESTAMP WITHOUT TIME ZONE, 
    creation_date TIMESTAMP WITHOUT TIME ZONE, 
    owner_id INTEGER, 
    globus_endpoint_id UUID, 
    num_nodes INTEGER, 
    backfill_windows JSON, 
    queued_jobs JSON, 
    optional_batch_job_params JSON, 
    allowed_projects JSON, 
    allowed_queues JSON, 
    transfer_locations JSONB, 
    PRIMARY KEY (id), 
    FOREIGN KEY(owner_id) REFERENCES users (id) ON DELETE CASCADE, 
    UNIQUE (hostname, path)
);

CREATE INDEX ix_sites_owner_id ON sites (owner_id);

CREATE TABLE apps (
    id SERIAL NOT NULL, 
    site_id INTEGER, 
    name VARCHAR(100) NOT NULL, 
    description TEXT, 
    class_path VARCHAR(200) NOT NULL, 
    parameters JSON, 
    transfers JSON, 
    PRIMARY KEY (id), 
    FOREIGN KEY(site_id) REFERENCES sites (id) ON DELETE CASCADE, 
    UNIQUE (site_id, class_path)
);

CREATE TABLE batch_jobs (
    id SERIAL NOT NULL, 
    site_id INTEGER, 
    scheduler_id INTEGER, 
    project VARCHAR(64), 
    queue VARCHAR(64), 
    optional_params JSON, 
    num_nodes INTEGER, 
    wall_time_min INTEGER, 
    job_mode VARCHAR(16), 
    filter_tags JSON, 
    state VARCHAR(16), 
    status_info JSON, 
    start_time TIMESTAMP WITHOUT TIME ZONE, 
    end_time TIMESTAMP WITHOUT TIME ZONE, 
    PRIMARY KEY (id), 
    FOREIGN KEY(site_id) REFERENCES sites (id) ON DELETE CASCADE
);

CREATE TABLE sessions (
    id SERIAL NOT NULL, 
    heartbeat TIMESTAMP WITHOUT TIME ZONE, 
    batch_job_id INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(batch_job_id) REFERENCES batch_jobs (id) ON DELETE CASCADE
);

CREATE TABLE jobs (
    id SERIAL NOT NULL, 
    workdir VARCHAR(256) NOT NULL, 
    tags JSONB, 
    app_id INTEGER, 
    session_id INTEGER, 
    parameters JSON, 
    batch_job_id INTEGER, 
    state VARCHAR(32), 
    last_update TIMESTAMP WITHOUT TIME ZONE, 
    data JSON, 
    return_code INTEGER, 
    num_nodes INTEGER, 
    ranks_per_node INTEGER, 
    threads_per_rank INTEGER, 
    threads_per_core INTEGER, 
    gpus_per_rank FLOAT, 
    node_packing_count INTEGER, 
    wall_time_min INTEGER, 
    launch_params JSON, 
    PRIMARY KEY (id), 
    FOREIGN KEY(app_id) REFERENCES apps (id) ON DELETE CASCADE, 
    FOREIGN KEY(batch_job_id) REFERENCES batch_jobs (id) ON DELETE SET NULL, 
    FOREIGN KEY(session_id) REFERENCES sessions (id) ON DELETE SET NULL
);

CREATE INDEX ix_jobs_id ON jobs (id);

CREATE INDEX ix_jobs_state ON jobs (state);

CREATE INDEX ix_jobs_tags ON jobs USING gin (tags);

CREATE TABLE job_deps (
    parent_id INTEGER NOT NULL, 
    child_id INTEGER NOT NULL, 
    PRIMARY KEY (parent_id, child_id), 
    FOREIGN KEY(child_id) REFERENCES jobs (id), 
    FOREIGN KEY(parent_id) REFERENCES jobs (id)
);

CREATE INDEX ix_job_deps_child_id ON job_deps (child_id);

CREATE INDEX ix_job_deps_parent_id ON job_deps (parent_id);

CREATE TABLE log_events (
    id SERIAL NOT NULL, 
    job_id INTEGER, 
    timestamp TIMESTAMP WITHOUT TIME ZONE, 
    from_state VARCHAR(32), 
    to_state VARCHAR(32), 
    data JSON, 
    PRIMARY KEY (id), 
    FOREIGN KEY(job_id) REFERENCES jobs (id) ON DELETE CASCADE
);

CREATE TABLE transfer_items (
    id SERIAL NOT NULL, 
    job_id INTEGER, 
    protocol VARCHAR(16), 
    remote_netloc VARCHAR(128), 
    source_path VARCHAR(256), 
    destination_path VARCHAR(256), 
    state VARCHAR(16), 
    task_id VARCHAR(100), 
    transfer_info JSON, 
    PRIMARY KEY (id), 
    FOREIGN KEY(job_id) REFERENCES jobs (id) ON DELETE CASCADE
);

INSERT INTO alembic_version (version_num) VALUES ('745d0fe434fc');

COMMIT;

