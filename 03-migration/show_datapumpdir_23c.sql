set pages 200 lines 200
select directory_name, directory_path
from dba_directories
where directory_name = 'DATA_PUMP_DIR';
exit
