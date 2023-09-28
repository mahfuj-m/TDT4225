import os
import numpy as np


###Preparation for the  User table
user_ids=dict()
user_activity=dict()
user_trackpoint=dict()
labeled_ids=[]
def User_Preparation():

    f=open('./dataset/dataset/labeled_ids.txt', 'r')
    for labeled_id in f:
        labeled_ids.append(labeled_id[:3])
    for user in os.listdir('./dataset/dataset/Data/'):
        if user!='.DS_Store':
            if user in labeled_ids:
                user_ids[user]=True
            else:
                user_ids[user]=False
    return user_ids
# user_ids=dict(sorted(user_ids.items()))


#### End of insertion

#preparation for the Activity Table

def Activity_dict():
    for root,dirs,files in os.walk('./dataset/dataset/Data/'):
        for dir in dirs:
            if dir in labeled_ids:
                path=os.path.join(root,dir,'labels.txt')
                data=np.genfromtxt(path, skip_header=1, delimiter='\t', usecols=(0,1,2),
                                   converters={
                                       0: (lambda x: str(x.decode('utf-8').replace('/','-'))),
                                       1: (lambda x: str(x.decode('utf-8').replace('/','-'))),
                                       2: (lambda x: str(x.decode('utf-8').replace('/','-'))),
                                   }).tolist()
                temp=list()

                for line in data:
                    updated_line=(line[0].replace("-", "").replace(" ","").replace(":", ""),line[1],line[2])
                    temp.append(updated_line)
                
                user_activity[dir]=temp
    
    return user_activity
    ###End of activity preparation


              
       

Activity_dict()
                


def Trackpoint_Preparation():
    for user in sorted(user_ids.keys()):
        activity=[]
        print('Processing : '+user)
        for root,dirs,files in os.walk('./dataset/dataset/Data/'+user):
            for file in files:
                
                transportation_mode=False
                if file.endswith(".plt"):
                    #print('File name: '+file)
                    if user in labeled_ids:
                        activity_data=user_activity.get(user)
                        for line in activity_data:
                            if file[:-4]==line[0]:
                                transportation_mode=line[2]
                                
                        
                    activity.append(np.genfromtxt(root + '/' + file,skip_header=6,delimiter=',',
                                                               usecols=(0, 1, 2, 3, 4, 5, 6),
                                                               converters={2: (lambda x: transportation_mode if transportation_mode else "NULL"),
                                                                           3: (lambda x: int(x) if isinstance(x, int) else float(x)),
                                                                           5: (lambda x: str(x.decode("utf-8"))),
                                                                           6: (lambda x: str(x.decode('utf-8')))},
                                                               ).tolist())  
                            
                    if(len(activity)>2500):
                        activity=activity[:2500]
        user_trackpoint[user]=activity
    return user_trackpoint
                
            
                            

                   
#Trackpoint_Preparation()





