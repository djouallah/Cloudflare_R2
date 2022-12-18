from datetime import datetime, date, timedelta
import pyarrow as pa
import pyarrow.dataset as ds
import pytz
import duckdb
from pyarrow import fs
import streamlit as st

def compaction(request):   
    cut_off=datetime.strftime(datetime.now(pytz.timezone('Australia/Brisbane')), '%Y-%m-%d')
    con=duckdb.connect()
    con.execute(f'''
        install httpfs;
        LOAD httpfs;
        set enable_progress_bar=false;
        PRAGMA enable_object_cache;
        SET enable_http_metadata_cache=true ;
        set s3_region = 'auto';
        set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
        set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
        set s3_endpoint = '{st.secrets["endpoint_url_secret"].replace("https://", "")}'  ;
        SET s3_url_style='path';
        create or replace view base  as select * from  parquet_scan('s3://delta/aemo/scada/data/*/*.parquet' , HIVE_PARTITIONING = 1,filename=1) where Date < '{cut_off}';
        create or replace view  filter as select Date, count(distinct filename) as cnt from  base  group by 1 having cnt>1 
        ''')
    
    tb=con.execute('''select SETTLEMENTDATE,DUID,SCADAVALUE,file,cast(base.Date as date) as Date from base inner join filter on base.Date= filter.Date''').arrow()
    
    s3 = fs.S3FileSystem(region="us-east-1",
                         access_key = st.secrets["aws_access_key_id_secret"],
                         secret_key=st.secrets["aws_secret_access_key_secret"] ,
                         endpoint_override=st.secrets["endpoint_url_secret"] )
        
    
    my_schema = pa.schema([
                      pa.field('SETTLEMENTDATE', pa.timestamp('us')),
                      pa.field('DUID', pa.string()),
                      pa.field('SCADAVALUE', pa.float64()),
                      pa.field('file', pa.string()),
                      pa.field('Date', pa.date32())
                         ])
    xx=tb.cast(target_schema=my_schema)
      
      
    ds.write_dataset(xx,"delta/aemo/scada/data/", filesystem=s3,format="parquet" , partitioning=['Date'],partitioning_flavor="hive",
     min_rows_per_group=120000,existing_data_behavior="delete_matching")
    
    return 'done'
