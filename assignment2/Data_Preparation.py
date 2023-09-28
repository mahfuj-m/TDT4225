import os
import numpy as np


###Preparation for the  User table
user_ids=dict()
labeled_ids=[]
f=open('../dataset/labeled_ids.txt', 'r')
for labeled_id in f:
    labeled_ids.append(labeled_id[:3])
for user in os.listdir('../dataset/Data/'):
    if user in labeled_ids:
        user_ids[user]=True
    else:
        user_ids[user]=False

#### End of insertion

#preparation for the Activity Table

def Activity_dict():
    count=0
    for root,dirs,files in os.walk('../dataset/Data/'):
        for dir in dirs:
            if dir in labeled_ids:
                path=os.path.join(root,dir,'labels.txt')
                data=np.genfromtxt(path, skip_header=1, delimiter='\t', usecols=(0,1,2),
                                   converters={
                                       0: (lambda x: str(x.decode('utf-8').replace('/','-'))),
                                       1: (lambda x: str(x.decode('utf-8').replace('/','-'))),
                                       2: (lambda x: str(x.decode('utf-8').replace('/','-'))),
                                   }).tolist()
                count=count+1
        if count==1:
            break
    print(data)
Activity_dict()
                



# for user in user_ids.keys():
#     activity=[]
#     for root,dirs,files in os.walk('../dataset/Data/'+user):
#         for file in files:
#             if file !='labels.txt':
#                 if(len(open(root+'/'+file,'r').readlines())>2506):
#                     if user in labeled_ids:
#                         activity_data=user
#                         #print(activity_data)

                   


#lines= sum(1 for _ in open('../dataset/Data/000/Trajectory/20081023025304.plt'))

# f=len(open('../dataset/Data/000/Trajectory/20081023025304.plt','r').readlines())
# print(f)



