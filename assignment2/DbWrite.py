from DbQuery import QueryClass

# Making Dictionary for creating tables in the DB

userDict={
    'id':'VARCHAR(3) NOT NULL PRIMARY KEY',
    'has_lebels': 'BOOLEAN'
}
activityDict={
'id': 'INT AUTO_INCREMENT NOT NULL PRIMARY KEY',
'user_id': 'varchar(3)',
'transportation_mode': 'varchar(20)',
'start_date_time': 'datetime',
'end_date_time': 'datetime',
'foreign key': '(user_id) references user(id)'

}
TrackpointDict={
'id': 'INT AUTO_INCREMENT NOT NULL PRIMARY KEY',
'activity_id': 'int',
'lat' : 'double',
'lon' : 'double',
'altitude': 'int',
'date_days':  'double',
'date_time': 'datetime',
'foreign key': '(activity_id) references activity(id)'
}

q=QueryClass()
q.create_table('user',userDict)
q.create_table('activity',activityDict)
q.create_table('trackpoint',TrackpointDict)
q.show_tables()
