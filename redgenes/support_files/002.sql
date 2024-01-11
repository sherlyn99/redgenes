-- cds_info table structure 
-- run_info save scripts or save individual information
BEGIN TRANSACTION;

create table if not exists identifier(
    entity_id integer primary key autoincrement,
    filename_full varchar not null, -- with suffix
    filepath varchar not null,
    active integer default 1,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    unique(filename_full, filepath)
);

create table if not exists md_info(
    md_id integer primary key autoincrement,
    entity_id integer not null, 
    source varchar not null,                 -- NCBI/external
    source_detailed varchar,        -- zengler
    external_accession varchar,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (entity_id) references identifier (entity_id)
);

create table if not exists run_info(
    run_id integer primary key autoincrement,
    software varchar,
    version varchar, 
    parameters varchar, 
    notes varchar,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp
);

-- store prodigal outputs
create table if not exists cds_info(
    cds_id integer primary key autoincrement, 
    entity_id integer not null,
    contig_id varchar not null,
    gene_id varchar,
    gene_type varchar not null,
    start integer not null,
    end integer not null,
    strand integer, 
    partial varchar,
    start_type varchar,
    stop_type varchar,
    rbs_motif varchar,
    rbs_spacer varchar,
    gc_cont real,
    conf real,
    score real,
    cscore real,
    sscore real,
    rscore real,
    uscore real,
    tscore real,
    mscore real,
    run_id integer, 
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (entity_id) references identifier (entity_id),
    foreign key (run_id) references run_info (run_id)
);

-- store kofam_scan outputs
create table if not exists ko_info(
    ko_id integer primary key autoincrement, 
    entity_id integer,
    gene_name varchar, 
    ko varchar,
    threshold real, 
    score real, 
    e_value real, 
    ko_definition varchar, 
    run_id integer,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (entity_id) references identifier (entity_id),
    foreign key (run_id) references run_info (run_id)
);

-- store barrnap outputs
create table if not exists rrna_info(
    rrna_id integer primary key autoincrement,
    entity_id integer, 
    contig_id varchar,
    rrna_type varchar,
    start int,
    end int,
    strand int, 
    source varchar, 
    score real, 
    product varchar, 
    note varchar,
    run_id integer,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (entity_id) references identifier (entity_id),
    foreign key (run_id) references run_info (run_id)
);

COMMIT;