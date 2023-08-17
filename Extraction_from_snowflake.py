def snowflake_data_extraction(user,password,account,warehouse,database,schema):
    # Snowflake connection parameters
    connection_params = {
        "user":user ,
        "password":password ,
        "account":account ,
        "warehouse":warehouse ,
        "database":database ,
        "schema":schema ,
    }

    # Snowflake query
    query = "SELECT DISTINCT USERS from REGION_EU.prep_retailers WHERE DEPARTMENT='HouseHold'"

    # Establish Snowflake connection
    conn = connect(**connection_params)

    # Read data from Snowflake
    df = pd.read_sql(query, conn)

    # Close Snowflake connection
    conn.close()

    return df

    