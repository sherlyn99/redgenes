-- cds_info table structure 
-- run_info save scripts or save individual information
BEGIN TRANSACTION;

create table if not exists identifier(
    genome_id integer primary key autoincrement,
    filename_full varchar not null, -- with suffix
    filepath varchar not null,
    active integer default 1,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    unique(filename_full, filepath)
);

create table if not exists md_info(
    md_id integer primary key autoincrement,
    genome_id integer, 
    source varchar,                 -- NCBI/external
    source_detailed varchar,        -- zengler
    external_accession varchar,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (genome_id) references identifier (genome_id)
);

-- store prodigal outputs
create table if not exists cds_info(
    cds_id integer primary key autoincrement, 
    genome_id integer,
    contig_id varchar,
    type varchar,
    start integer,
    end integer,
    score real, 
    strand varchar, 
    phase integer, 
    attributions integer,
    run_id integer, 
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (genome_id) references identifier (genome_id),
    foreign key (run_id) references run_info (run_id)
);

-- store kofam_scan outputs
create table if not exists ko_info(
    ko_id integer primary key autoincrement, 
    genome_id integer,
    gene_name varchar, 
    ko varchar,
    threshold real, 
    score real, 
    evalue real, 
    ko_definition varchar, 
    run_id integer,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (genome_id) references identifier (genome_id),
    foreign key (run_id) references run_info (run_id)
);

-- store barrnap outputs
create table if not exists rrna_info(
    rrna_id integer primary key autoincrement,
    genome_id integer, 
    contig_id varchar,
    source varchar,
    type varchar,
    start int,
    end int,
    score real,
    strand varchar,
    phase varchar,
    run_id integer,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    foreign key (genome_id) references identifier (genome_id),
    foreign key (run_id) references run_info (run_id)
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

COMMIT;