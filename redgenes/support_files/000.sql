-- create table: settings
begin;

create table if not exists settings(
    patch_id integer,
    executed integer default 0,
    created_at timestamp default current_timestamp,  -- utc time
    modified_at timestamp default current_timestamp, -- utc time
    unique(patch_id)
);

commit;