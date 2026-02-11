set pages 200 lines 200 trimspool on
spool /tmp/create_legacy_user_23c.log

prompt === Switch to PDB ===
alter session set container=FREEPDB1;

prompt === Create user + grants ===
begin
  execute immediate 'create user legacy_app identified by legacy123';
exception when others then
  if sqlcode = -01920 then null; else raise; end if;
end;
/
grant connect, resource to legacy_app;
alter user legacy_app quota unlimited on users;

prompt === Verify user exists in PDB ===
select username from dba_users where username='LEGACY_APP';

spool off
exit
