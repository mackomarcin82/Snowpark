#import required libraries

import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col 


#establishing a connection

def create_session_object():
    connection_parameters = {
        "account": "QEZHSEM-ZMB90776",
        "user": "MACKOMARCIN82",
        "password": "Karamazow19822",
        "role": "SYSADMIN",  # optional
        "warehouse": "COMPUTE_WH",  # optional
        "database": "AGS_GAME_AUDIENCE",  # optional
        "schema": "ENHANCED",  # optional
}
    
    new_session = Session.builder.configs(connection_parameters).create()
    return new_session

    
    
def create_dataframe(session):
  
    # Create a dataframe
    df_table = session.table("AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED")

    #---------------------------------
    # **ACTIONS**
    #---------------------------------

    # count method
    df_table.count()
    # print(df_table.count()) 
  
    # show method
    df_table.show()
  
    # collect method
    df_results = df_table.collect()
    # print(df_results) 
  
    #---------------------------------
    # **TRANSFORMATIONS **
    #---------------------------------
  
    df_filtered = df_table.filter(col("City") == 'Warsaw')
  
    # Chaining method calls
    # df_filtered = df_table.filter(col("CS_BILL_CUSTOMER_SK") > 6000000).sort(col("CS_SHIP_ADDR_SK").desc()).limit(2000000)

    df_filtered.show()
  
    df_filtered.collect()
  
    df_filtered_persisted = df_filtered.collect()
    # print(df_filtered_persisted)



# call session object
session = create_session_object()

# call create dataframe 
_ = create_dataframe(session) 

# end your session
session.close()