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
    foreign key (entity_id) references identifier (entity_id),
    unique(md_id, entity_id)
);

create table if not exists run_info(
    run_id integer primary key autoincrement,
    software varchar not null,
    version varchar not null,
    commands varchar not null, 
    notes varchar,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp, 
    unique(software, version, commands)
);

-- store prodigal outputs
create table if not exists cds_info(
    cds_id integer primary key autoincrement, 
    entity_id integer not null,
    contig_id varchar not null,
    gene_id varchar not null,
    gene_type varchar not null,
    start integer not null,
    end integer not null,
    conf real not null,    
    score real not null,
    source varchar not null,
    strand integer not null,
    phase integer not null, 
    partial varchar,
    start_type varchar,
    stop_type varchar,
    rbs_motif varchar,
    rbs_spacer varchar,
    gc_cont real,
    cscore real,
    sscore real,
    rscore real,
    uscore real,
    tscore real,
    mscore real,
    start_fuzzy varchar not null, 
    end_fuzzy varchar not null,
    run_id integer, 
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (entity_id) references identifier (entity_id),
    foreign key (run_id) references run_info (run_id),
    unique(entity_id, contig_id, gene_id, gene_type, start)
);

-- store kofam_scan outputs
create table if not exists ko_info(
    ko_id integer primary key autoincrement, 
    entity_id integer not null,
    gene_name varchar not null, 
    ko varchar not null,
    threshold real not null, 
    score real not null, 
    e_value real not null, 
    ko_definition varchar not null, 
    run_id integer,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (entity_id) references identifier (entity_id),
    foreign key (run_id) references run_info (run_id),
    unique(entity_id, gene_name, ko, threshold)
);

-- store barrnap outputs
create table if not exists rrna_info(
    rrna_id integer primary key autoincrement,
    entity_id integer not null, 
    contig_id varchar not null,
    gene_type varchar not null,
    rrna_name varchar not null,
    start int not null,
    end int not null,
    strand int not null, 
    source varchar not null, 
    score real not null, 
    product varchar, 
    note varchar,
    start_fuzzy varchar not null, -- 1: True, 0: False
    end_fuzzy varchar not null, -- 1: True, 0: False
    run_id integer,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (entity_id) references identifier (entity_id),
    foreign key (run_id) references run_info (run_id),
    unique(entity_id, contig_id, rrna_name, start)
);

COMMIT;