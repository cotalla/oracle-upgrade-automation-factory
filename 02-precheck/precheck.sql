set pages 200 lines 200 trimspool on
spool /tmp/precheck.log

prompt === DATABASE & PDB STATUS ===
select name, open_mode from v$database;
select name, open_mode from v$pdbs;

prompt === VERSION ===
select * from v$version;

prompt === CHARACTER SET ===
select parameter, value
from nls_database_parameters
where parameter in ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET');

prompt === COMPONENTS (DBA_REGISTRY) ===
select comp_id, comp_name, version, status
from dba_registry
order by comp_id;

prompt === INVALID OBJECTS SUMMARY ===
select owner, count(*) invalid_count
from dba_objects
where status <> 'VALID'
group by owner
order by invalid_count desc;

spool off
exit
