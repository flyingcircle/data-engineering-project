CREATE SEQUENCE trip_id_seq;
alter table "Trip" add column id integer default nextval('trip_id_seq');
CREATE SEQUENCE breadcrumb_id_seq;
alter table "BreadCrumb" add column id integer default nextval('breadcrumb_id_seq');

-- Uniquify the Trip table.
-- delete from "Trip" a using "Trip" b where a.id < b.id and a.trip_id = b.trip_id and a.route_id = b.route_id;

-- Uniquify the BreadCrumb table.
-- delete from "BreadCrumb" a using "BreadCrumb" b where a.id < b.id and a.trip_id = b.trip_id and a.tstamp = b.tstamp;

CREATE View FIXED_TRIP as
  SELECT t.trip_id, t.route_id, t.vehicle_id, s.service_key, s.direction, 
  FROM "Trip" as t, "Stop" as s
  WHERE t.trip_id = s.trip_id;