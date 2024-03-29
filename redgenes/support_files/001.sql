---------------------------
-- create tables
---------------------------
BEGIN TRANSACTION;

-- create table: bakta
CREATE TABLE IF NOT EXISTS bakta (
    bakta_accession integer primary key autoincrement,
    entity_id varchar,
    contig_id varchar,
    gene_id varchar,
    source varchar,
    type varchar,
    start integer,
    end integer,
    strand varchar,
    phase varchar,
    name varchar,
    product varchar,
    RefSeq varchar,
    SO varchar,
    UniParc varchar,
    Uniref varchar,
    KEGG varchar,
    PFAM varchar,
    run_accession integer,
    created_at timestamp default current_timestamp not null,
    updated_at timestamp default current_timestamp not null,
    foreign key (run_accession) references run_info (run_accession),
    foreign key (entity_id) references identifier (entity_id)
);

-- create table: quast
-- may reduce the number of fields in the future
CREATE TABLE IF NOT EXISTS quast (
    quast_accession integer primary key autoincrement,
    entity_id varchar,
    total_length integer,
    largest_contig integer,
    ncontigs integer,
    gc integer,
    n50 integer,
    n75 integer,
    aun integer,
    l50 integer,
    l75 integer,
    ncontigs_0bp integer,
    ncontigs_1000bp integer,
    ncontigs_5000bp integer,
    ncontigs_10000bp integer,
    ncontigs_25000bp integer,
    ncontigs_50000bp integer,
    total_length_0bp integer,
    total_length_1000bp integer,
    total_length_5000bp integer,
    total_length_10000bp integer,
    total_length_25000bp integer,
    total_length_50000bp integer,
    nN_per_100kbp integer,
    run_accession integer,
    created_at timestamp default current_timestamp not null,
    updated_at timestamp default current_timestamp not null,
    foreign key (run_accession) references run_info (run_accession),
    foreign key (entity_id) references identifier (entity_id)
);

COMMIT;