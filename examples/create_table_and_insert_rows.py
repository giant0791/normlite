from datetime import date

from normlite import (
    create_engine, 
    insert, 
    select,
    Table, 
    Column,
    String, 
    Integer, 
    Boolean, 
    Date, 
    MetaData,
)

# create bind to in-memory database
engine = create_engine('normlite:///:memory:')

# declare a table
metadata = MetaData()
students = Table(
    'students',
    metadata,
    Column('id', Integer()),
    Column('name', String(is_title=True)),
    Column('grade', String()),
    Column('is_active', Boolean()),
    Column('started_on', Date()),
)

# create table and add some rows
with engine.connect() as connection:
    students.create(bind=engine)
    
    stmt = (
        insert(students)
        .values(
            id=123456, 
            name='Galileo Galilei', 
            grade='A', 
            is_active=False, 
            started_on=date(1581, 9, 1)
        )
    )

    connection.execute(stmt)
    
    stmt = students.insert()
    connection.execute(
        stmt, 
        {
            'id': 123457, 
            'name': 'Isaac Newton', 
            'grade': 'B', 
            'is_active': False, 
            'started_on': date(1661, 9, 1)
        }
    )

    stmt = (
        select(students)
        .where(students.c.is_active.is_not(True))
    )
    result = connection.execute(stmt)
    rows = result.all()

    # print values for user defined columns only
    for row in rows:
        print(row)