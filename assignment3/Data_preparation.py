import os
from Db_Interaction import Database
from tabulate import tabulate
from datetime import datetime




os.chdir("..")
current_location =  os.getcwd()
dataset_location = os.path.join(current_location,"dataset", "dataset")


user_docs=[]

trackpoint_docs=[]

def generate_leveled_ids():
    labeled_ids=[]
    f=open(os.path.join(dataset_location,'labeled_ids.txt'), 'r')
    for labeled_id in f:
        labeled_ids.append(labeled_id[:3])
    return labeled_ids


def user_collection():
    labeled_ids=generate_leveled_ids()
    for user in os.listdir(os.path.join(dataset_location,'Data')):
        if user!='.DS_Store':
            has_label= True if user in labeled_ids else False
            user_docs.append({
                    '_id':          user, 
                    'has_label':    has_label
                })

    return user_docs
def input_users(db):
    user_docs=user_collection()
    colls=db.show_coll()
    if 'users' in colls:
        db.drop_coll('users')
    db.create_coll('users')
    db.insert_documents('users',user_docs)
    print(" users inserted")

def activity_collection():
    labeled_ids=generate_leveled_ids()
    activity=dict()
    count=1
    for root,dirs,files in os.walk(os.path.join(dataset_location,'Data'),topdown=False):
        count=0

        for dir in dirs:
            activity_docs=[]
            if dir in labeled_ids:
                path=os.path.join(root,dir,'labels.txt')
                ac=open(path,'r')
                rows=ac.readlines()[1:]
                
                for row in rows:
                    array=row.strip().split("\t")
                    #array.append(dir)
                    
                    activity_docs.append({
                    '_id':  count, 
                    'transportation_mode': array[2],
                    'start_date_time': array[0],
                    'end_date_time': array[1],
                    'user_id':  dir,
                     })
                    #print(count)
                    count+=1
                activity[dir]=activity_docs

    return activity

def create_activity_coll(db):
    colls=db.show_coll()
    if 'activity' in colls:
        db.drop_coll('activity')
    db.create_coll_by_index('activity')

def insert_activity(db):
    activity=activity_collection()
    for user, ac in sorted(activity.items()):
        db.insert_documents('activity',ac)


def create_trackpoint_coll(db):
    colls=db.show_coll()
    if 'trackpoint' in colls:
        db.drop_coll('trackpoint')
    db.create_coll('trackpoint')


def trackpoint_collection():
    count=0
    users=generate_leveled_ids()
    trackpoint_colls=dict()
    for user in sorted(users):
        trackpoints=list()
        print('Processing User: '+user)
        for root,dirs,files in os.walk(os.path.join(dataset_location,'Data',user)):
            for file in files:
                if (file.endswith('.plt') and file != '.DS_Store'):
                    if (sum(1 for _ in open(root+'/'+file))<=2500):
                        tj_data=open(root+'/'+file,'r')
                        tj_rows=tj_data.readlines()[6:]
                        for row in tj_rows:
                            array=row.strip().split("\t")[0].split(',')
                            trackpoints.append({
                                "lat": array[0],
                                'lon':array[1],
                                'altitude': 0 if array[3]==-777 else array[3],
                                'date_days':array[4],
                                'date_time':array[-2]+" "+array[-1]
                            })
        trackpoint_colls[user]=trackpoints
        # if count==4:
        #     break
        count+=1

    return trackpoint_colls
def date_formation(date):
    return datetime.strptime(date,"%Y-%m-%d %H:%M:%S")

def insert_trackpoint(db):
    trackpoints=trackpoint_collection()
    for user, trackpoint in trackpoints.items():
        activity=db.get_activity({'user_id': user})
        trackpoint_with_activity=list()
        tp_tuple=tuple()
        count=0
        for tp in trackpoint:
            time= date_formation(tp['date_time'])
            activity_id=[a['_id'] for a in activity if(date_formation(a['start_date_time'].replace("/","-")) <= time <= date_formation(a['end_date_time'].replace("/","-")))]
            if activity_id:
                #Make a insert dictionary
                tp_tuple=(activity_id[0])
            if tp_tuple:
                trackpoint_with_activity.append({
                    "lat":tp['lat'],
                    "lon":tp['lon'],
                    'altitude':tp['altitude'],
                    'date_days':tp['date_days'],
                    'date_time':tp['date_time'],
                    'activity_id': tp_tuple
                })
            count+=1
        print(user+"   "+str(len(trackpoint_with_activity)))
        if trackpoint_with_activity:
            db.insert_documents('trackpoint',trackpoint_with_activity)
            print(user+" added trackpoints : "+str(count))
        else:
            print(user+" added  0 trackpoints")
            
        

            


        #Read user's activity by user_id '2008-09-27 23:47:31'





        
            
                    


                    


db=Database()
input_users(db)  #step 1
#print(generate_leveled_ids())
#db.fetch_documents('activity')
#print(db.unique_items('activity','user_id'))


create_activity_coll(db) # step 2
#print(db.items('activity'))
insert_activity(db)



#trackpoint_collection()

create_trackpoint_coll(db)  #step 3


insert_trackpoint(db)
#db.fetch_documents('trackpoint')



