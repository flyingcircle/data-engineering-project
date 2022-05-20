drop table if exists Stop;
drop type if exists service_type;

create type service_type as enum('Weekday', 'Saturday', 'Sunday');

create table Stop(
    trip_id integer,
    route_id integer,
    service_key service_type,
    direction integer
);