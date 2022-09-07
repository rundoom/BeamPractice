create table if not exists measurement_event
(
    user_id          integer,
    value            double precision,
    measurement_type varchar(16),
    location         integer,
    timestamp        bigint
);

alter table measurement_event
    owner to postgres;