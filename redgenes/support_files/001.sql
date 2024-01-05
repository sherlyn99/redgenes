---------------------------
-- create tables
---------------------------
-- BEGIN TRANSACTION;

-- -- create table: identifier
-- CREATE TABLE IF NOT EXISTS identifier(
--     entity_id integer primary key autoincrement,
--     filename varchar not null,
--     filepath varchar not null,
--     external_accession varchar,
--     external_source varchar,
--     active int default 1,
--     created_at timestamp default current_timestamp,
--     unique(filename, filepath),
--     unique(external_accession, external_source)
-- );

-- -- create table: run_info
-- CREATE TABLE IF NOT EXISTS run_info(
--             run_accession integer primary key autoincrement,
--             slurm_job_id varchar,
--             software_accession integer,
--             run_at timestamp default current_timestamp,
--             foreign key (software_accession) references software_info (software_accession)
--         );

-- -- create table: software_info
-- -- constraint: unique(software_name, version, arguments)
-- CREATE TABLE IF NOT EXISTS software_info (
--     software_accession integer primary key autoincrement,
--     software_name varchar,
--     version varchar,
--     arguments varchar,
--     description varchar,
--     created_at timestamp default current_timestamp,
--     unique(software_name, version, arguments)
-- );

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
    created_at timestamp default current_timestamp,
    foreign key (run_accession) references run_info (run_accession),
    foreign key (entity_id) references identifier (entity_id)
);

-- -- create table: quast
-- -- may reduce the number of fields in the future
-- CREATE TABLE IF NOT EXISTS quast (
--     quast_accession integer primary key autoincrement,
--     entity_id varchar,
--     total_length integer,
--     largest_contig integer,
--     ncontigs integer,
--     gc integer,
--     n50 integer,
--     n75 integer,
--     aun integer,
--     l50 integer,
--     l75 integer,
--     ncontigs_0bp integer,
--     ncontigs_1000bp integer,
--     ncontigs_5000bp integer,
--     ncontigs_10000bp integer,
--     ncontigs_25000bp integer,
--     ncontigs_50000bp integer,
--     total_length_0bp integer,
--     total_length_1000bp integer,
--     total_length_5000bp integer,
--     total_length_10000bp integer,
--     total_length_25000bp integer,
--     total_length_50000bp integer,
--     nN_per_100kbp integer,
--     run_accession integer,
--     created_at timestamp default current_timestamp,
--     foreign key (run_accession) references run_info (run_accession),
--     foreign key (entity_id) references identifier (entity_id)
-- );

-- -- -- create table: metadata
-- -- CREATE TABLE IF NOT EXISTS metadata (
-- --     md_id integer primary key autoincrement,
-- --     entity_id integer,
-- --     external_source varchar,
-- --     external_accession varchar,
-- --     bioproject varchar,
-- --     biosample varchar,
-- --     wgs_master varchar,
-- --     refseq_category varchar,
-- --     taxid integer,
-- --     species_taxid varchar,
-- --     organism_name varchar,
-- --     infraspecific_name varchar,
-- --     isolate varchar,
-- --     version_status varchar,
-- --     assembly_level varchar,
-- --     release_type varchar,
-- --     genome_rep varchar,
-- --     seq_rel_date varchar,
-- --     asm_name varchar,
-- --     submitter varchar,
-- --     gbrs_paired_asm varchar,
-- --     paired_asm_comp varchar,
-- --     ftp_path varchar,
-- --     excluded_from_refseq varchar,
-- --     relation_to_type_material varchar,
-- --     asm_not_live_date varchar,
-- --     created_at timestamp default current_timestamp,
-- --     foreign key (entity_id) references identifier (entity_id)
-- -- );

-- COMMIT;