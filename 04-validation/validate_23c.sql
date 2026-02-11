set pages 200 lines 200 trimspool on
spool /tmp/validation_23c.log

prompt === CURRENT CONTAINER ===
select sys_context('USERENV','CON_NAME') as con_name from dual;

prompt === ROW COUNT CHECK ===
select count(*) as orders_count from legacy_app.orders;

prompt === INVALID OBJECTS (EXPECTED: BAD_VIEW) ===
select owner, object_name, object_type, status
from dba_objects
where owner='LEGACY_APP'
  and status <> 'VALID'
order by object_name;

prompt === SEQUENCE TEST ===
select legacy_app.order_seq.nextval as next_seq from dual;

spool off
exit
