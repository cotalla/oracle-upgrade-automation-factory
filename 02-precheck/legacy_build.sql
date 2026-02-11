set pages 200 lines 200 trimspool on
spool /tmp/legacy_build.log

prompt === CREATE LEGACY APP USER (IDEMPOTENT) ===
begin
  execute immediate 'create user legacy_app identified by legacy123';
exception
  when others then
    if sqlcode = -01920 then null; else raise; end if;
end;
/
begin
  execute immediate 'grant connect, resource to legacy_app';
exception when others then null;
end;
/
begin
  execute immediate 'alter user legacy_app quota unlimited on users';
exception when others then null;
end;
/

prompt === CREATE TABLE (IF NOT EXISTS) ===
begin
  execute immediate '
    create table legacy_app.orders (
      order_id   number primary key,
      order_date date,
      amount     number(10,2)
    )';
exception
  when others then
    if sqlcode = -955 then null; else raise; end if;
end;
/

prompt === LOAD DATA (ONLY IF TABLE EMPTY) ===
declare
  c number;
begin
  select count(*) into c from legacy_app.orders;
  if c = 0 then
    insert into legacy_app.orders
    select level, sysdate - level, level * 10
    from dual connect by level <= 50000;
    commit;
  end if;
end;
/

prompt === INTENTIONAL INVALID OBJECT (REALISTIC UPGRADE PAIN) ===
create or replace view legacy_app.bad_view as
select nonexistent_column from legacy_app.orders;

prompt === SEQUENCE (IF NOT EXISTS) ===
begin
  execute immediate 'create sequence legacy_app.order_seq start with 50001';
exception
  when others then
    if sqlcode = -955 then null; else raise; end if;
end;
/

prompt === EVIDENCE QUERIES ===
select count(*) as orders_count from legacy_app.orders;

select owner, object_name, object_type, status
from dba_objects
where owner='LEGACY_APP'
  and status <> 'VALID'
order by object_name;

spool off
exit
