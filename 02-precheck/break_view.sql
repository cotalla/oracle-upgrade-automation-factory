set pages 200 lines 200 trimspool on
spool /tmp/break_view.log

prompt === CREATE VIEW VALID FIRST ===
create or replace view legacy_app.bad_view as
select order_id, amount from legacy_app.orders;

prompt === BREAK THE VIEW (DROP COLUMN) ===
alter table legacy_app.orders drop column amount;

prompt === SHOW INVALID OBJECTS ===
select owner, object_name, object_type, status
from dba_objects
where owner='LEGACY_APP'
  and status <> 'VALID'
order by object_name;

spool off
exit
