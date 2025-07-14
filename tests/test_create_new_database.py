import pytest

from normlite.engine import Engine, create_engine
"""
Quick mapping between Notion and the database world.

Database store: This a dedicated Notion workspace as the entrypoint where all databases and metadata will be stored.
Think of it as a file system residing on the cloud.

information_schema: Top Notion page in the workspace where all table metadata are stored.

tables: Notion database for storing all table metadata in the store. 
The tables schema is as follows:
  - table_name: name of a database table
  - table_catalog: name of the database to which the table belongs. 
    A table catalog is a Notion page at the top of the workspace
  - table_id: Notion object id of the Notion database corresponding to the table 

Database: A database is a Notion top page. All tables belonging to this database are 
Notion databases contained in this page. 

Create a new database (new Notion page)
1. Search for a Notion page with the database name
2. No page is found, so create a new page

Create a new table (new Notion database)
1. Create a new Notion database under the page respresenting the current database
2. Update the tables Notion database under the page information_schema
"""



def test_create_new_database_with_params():
    """Test that a new database can be created"""
    
    # create an engine to connect to the database server
    engine: Engine = create_engine(
        uri='normlite://',
        host='www.notion.so', 
        database='testdatabase',
        api_key = 'ntn_abc123def456ghi789jkl012mno345pqr' 
    )


@pytest.mark.skip(reason="execute() method not implemented in Engine class yet")
def test_create_new_table():
    """Test that a new table can be created in the database"""

    # create an engine to connect to the database server
    engine: Engine = create_engine(
        uri='normlite://',
        host='www.notion.so', 
        database='testdatabase',
        api_key = 'ntn_abc123def456ghi789jkl012mno345pqr' 
    )
    connection: Connection = engine.connect()
    connection.execute(
        'create table students (id int, name varchar(255), grade varchar(1))'
    )











