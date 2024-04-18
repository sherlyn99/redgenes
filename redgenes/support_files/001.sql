---------------------------
-- create tables
---------------------------
BEGIN TRANSACTION;
create table if not exists identifier(
    entity_id integer primary key autoincrement,
    filename_full varchar not null, -- with suffix
    filepath varchar not null,
    active integer default 1,
    created_at timestamp default current_timestamp not null,
    modified_at timestamp default current_timestamp not null,
    unique(filename_full, filepath)
);

create table if not exists md_info(
    md_id integer primary key autoincrement,
    entity_id integer not null, 
    source varchar,
    source_detailed varchar,
    external_accession varchar,
    created_at timestamp default current_timestamp not null,
    modified_at timestamp default current_timestamp not null,
    foreign key (entity_id) references identifier (entity_id),
    unique(md_id, entity_id)
);

create table if not exists run_info(
    run_id integer primary key autoincrement,
    software varchar not null,
    version varchar not null,
    commands varchar not null, 
    notes varchar,
    created_at timestamp default current_timestamp not null,
    modified_at timestamp default current_timestamp not null, 
    unique(software, version, commands)
);

CREATE TABLE IF NOT EXISTS gene_accession_counter (
    last_used_id integer not null
);
INSERT INTO gene_accession_counter (last_used_id)
SELECT 0 WHERE NOT EXISTS (SELECT * FROM gene_accession_counter);

CREATE TABLE IF NOT EXISTS bakta (
    bakta_accession integer primary key autoincrement,
    entity_id integer,
    contig_id varchar,
    type varchar,
    gene_id varchar,
    start integer,
    stop integer,
    strand varchar,
    locus_tag varchar,
    gene varchar,
    product varchar,
    gene_accession integer,
    created_at timestamp default current_timestamp not null,
    updated_at timestamp default current_timestamp not null,
    foreign key (entity_id) references identifier (entity_id)
);

CREATE TABLE IF NOT EXISTS refseq (
    refseq_accession INTEGER PRIMARY KEY AUTOINCREMENT,
    bakta_accession INTEGER NOT NULL,
    RefSeq varchar NOT NULL,
    foreign key (bakta_accession) references bakta (bakta_accession)
);

CREATE TABLE IF NOT EXISTS so (
    so_accession INTEGER PRIMARY KEY AUTOINCREMENT,
    bakta_accession INTEGER NOT NULL,
    SO varchar NOT NULL,
    foreign key (bakta_accession) references bakta (bakta_accession)
);

CREATE TABLE IF NOT EXISTS uniparc (
    uniparc_accession INTEGER PRIMARY KEY AUTOINCREMENT,
    bakta_accession INTEGER NOT NULL,
    UniParc varchar NOT NULL,
    foreign key (bakta_accession) references bakta (bakta_accession)
);

CREATE TABLE IF NOT EXISTS uniref (
    uniref_accession INTEGER PRIMARY KEY AUTOINCREMENT,
    bakta_accession INTEGER NOT NULL,
    UniRef varchar NOT NULL,
    foreign key (bakta_accession) references bakta (bakta_accession)
);

CREATE TABLE IF NOT EXISTS kegg (
    kegg_accession INTEGER PRIMARY KEY AUTOINCREMENT,
    bakta_accession INTEGER NOT NULL,
    KEGG varchar NOT NULL,
    foreign key (bakta_accession) references bakta (bakta_accession)
);

CREATE TABLE IF NOT EXISTS pfam (
    pfam_accession INTEGER PRIMARY KEY AUTOINCREMENT,
    bakta_accession INTEGER NOT NULL,
    PFAM varchar NOT NULL,
    foreign key (bakta_accession) references bakta (bakta_accession)
);

CREATE TABLE IF NOT EXISTS embedding (
    embedding_accession INTEGER PRIMARY KEY AUTOINCREMENT,
    dimension INTEGER NOT NULL,
    embedding REAL NOT NULL,
    bakta_accession INTEGER NOT NULL,
    foreign key (bakta_accession) references bakta (bakta_accession)
);

CREATE INDEX IF NOT EXISTS idx_bakta_accession ON bakta(bakta_accession);
CREATE INDEX IF NOT EXISTS idx_refseq ON refseq(RefSeq);
CREATE INDEX IF NOT EXISTS idx_uniref ON uniref(UniRef);
CREATE INDEX IF NOT EXISTS idx_uniparc ON uniparc(UniParc);

create table if not exists qc_info(
    qc_id integer primary key autoincrement, 
    entity_id integer not null,
    marker_lineage varchar,
    completeness float not null,
    contamination float not null,
    num_scaffolds integer not null,
    num_contigs integer not null,
    longest_scaffold integer not null,
    longest_contig integer not null,
    N50_scaffolds integer not null,
    N50_contigs integer not null,
    mean_scaffold_length float not null,
    mean_contig_length float not null,
    coding_density float not null,
    translation_table integer not null,
    num_predicted_genes integer not null,
    created_at timestamp default current_timestamp not null,
    modified_at timestamp default current_timestamp not null,
    foreign key (entity_id) references identifier (entity_id),
    unique(entity_id)
);

COMMIT;