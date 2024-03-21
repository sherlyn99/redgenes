BEGIN TRANSACTION;

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
    run_id integer not null,
    created_at timestamp default current_timestamp not null,
    modified_at timestamp default current_timestamp not null,
    foreign key (entity_id) references identifier (entity_id),
    foreign key (run_id) references run_info (run_id),
    unique(entity_id, run_id)
);

COMMIT;