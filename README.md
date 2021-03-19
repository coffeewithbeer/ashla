# ASHLA #

Package to deal with Wide Binary stuff.

# Installation #

Firstly, clone the repository onto your local machine. Say the clone lives in
 C:/git/ashla
 
    cd C:/git/ashla
    pip install .

# Usage #

#### ashla.data_access (Gaia Access)

Get data from gaia as a Pandas DataFrame:

    import ashla.data_access as da

    data = da.query_gaia_to_pandas(query, login_conf=None)
    
For logging in, you can use the ESA login and password. Either 
don't supply a login_conf object and you will be promted to enter 
a login and password in the terminal. Else make a copy of the ashla/data_access/login_config.ini 
file, and enter username and password there. Then add a link to this config file as the 
login_conf variable. 

For example:

> We make a copy of login_conf.ini in C:\configs, containing:

    [login]
    user = my_username
    password = my_password
    
This is all the inputs we need. 

We can then call these functions using the file path of this config.

    data = da.query_gaia_to_pandas(query, r'C:\configs\login_config.ini')

The data will now be in a Pandas DataFrame format. You can additionally query
and save as a Parquet file named gaia_data.parquet.gzip using:

    gaia_query_to_parquet(query, 'gaia_data', login_cnf=r'C:\configs\login_config.ini')
    
Again, here you can leave login_cnf empty and enter login details in the terminal.


    