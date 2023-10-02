from DbQuery import QueryClass
from Data_Preparation import *
from datetime import datetime

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


def insert_Users(q,users):
    users_list=list()
    for user,label in sorted(users.items()):
        users_list.append((user,label))
    query="INSERT INTO user (id,has_lebels) VALUES (%s, %s)"
    #print(users_list[:5])
    q.insert_data(query,users_list)


def insert_Activity(q,activity):
    activities=list()
    for user,ac in sorted(activity.items()):
        if isinstance(ac, tuple):
            updated_ac=[(user,ac[2],ac[0],ac[1])]
        else:
            updated_ac=[(user,x[2],x[0],x[1]) for x in ac]
        query="INSERT INTO activity (user_id,transportation_mode,start_date_time,end_date_time) VALUES (%s, %s, %s, %s)"
        print("Inserting :"+ user)
        #print(updated_ac)
        q.insert_data(query,updated_ac)



def insert_Trackpoint(q,trackpoints):
    for user,trackpoint in trackpoints.items():
        query = "SELECT id, start_date_time, end_date_time  FROM activity WHERE user_id = '" + user + "';"
        #query = "SELECT id, start_date_time, end_date_time  FROM activity WHERE user_id = '128';"
        # Use pandas
        activity=q.read_data(query)

        activity_list=list()
        tp_tuple=tuple()
        for tp in trackpoint:
            time=datetime.strptime(tp[5]+" "+tp[6], "%Y-%m-%d %H:%M:%S")
            activity_index=[a[0] for a in activity if(a[1] <=  time <= a[2])]
            if activity_index:
                tp_tuple=(activity_index[0],tp[0],tp[1],int(tp[3]),tp[4],str(tp[5] + " " + tp[6]))
            if tp_tuple: ###Remove the empty tuple
                activity_list.append(tp_tuple)
        print(user +" :  "+str(len(activity_list)))
        if activity_list:
            if len(activity_list)>500000:
                chunks_size=200000
                acitivity_chunks=list()
                for i in range(0, len(activity_list), chunks_size):
                    sublist=activity_list[i:i+chunks_size]
                    acitivity_chunks.append(sublist)
                for item in acitivity_chunks:
                    print(len(item))
                    query="INSERT INTO trackpoint (activity_id,lat,lon,altitude, date_days,date_time) VALUES (%s, %s, %s, %s,%s , %s)"
                    q.insert_data(query,item)
            else:
                query="INSERT INTO trackpoint (activity_id,lat,lon,altitude, date_days,date_time) VALUES (%s, %s, %s, %s,%s , %s)"
                print("Inserting :"+ user+ " trackpoints")
                q.insert_data(query,activity_list)
        else:
            print(user + " No trackpoints available")



#     # query = "SELECT id, start_date_time, end_date_time  FROM activity WHERE user_id = '052';"
#     # activity=q.read_data(query)

#     # activity_list=list()
#     # tp_tuple=tuple()
#     # for tp in trackpoints['052']:
#     #     time=datetime.strptime(tp[5]+" "+tp[6], "%Y-%m-%d %H:%M:%S")
#     #     activity_index=[a[0] for a in activity if(a[1] <=  time <= a[2])]
#     #     if activity_index:
#     #         tp_tuple=(activity_index[0],tp[0],tp[1],int(tp[3]),tp[4],str(tp[5] + " " + tp[6]))
#     #     activity_list.append(tp_tuple)
#     # print(activity_list)
#         # if user in labeled_ids:
#         #     pass
#         #     #Perform abnormal operation
#         # else:
#         #     pass
#         #     #Write code if you want to add the trackpoints that don't have activity data
#             # print(trackpoint)
#             #Perform normal operation
#         #print(user)
#     #     query = "SELECT * FROM activity WHERE user_id = '" + user + "';"

#     #     print(q.read_data(query))
#     # query = "SELECT * FROM activity WHERE user_id = '010';"
#     # print(q.read_data(query))
  




q=QueryClass()
q.create_table('user',userDict)
q.create_table('activity',activityDict)
q.create_table('trackpoint',TrackpointDict)
#q.show_tables()
users=User_Preparation()
insert_Users(q,users)
activity,_=Activity_dict()
insert_Activity(q,activity)
user_trackpoints=Trackpoint_Preparation()
insert_Trackpoint(q,user_trackpoint)
# # q.delete_table(['trackpoint','activity','user'])
# # q.show_tables()
