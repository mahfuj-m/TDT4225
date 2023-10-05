from DbQuery import QueryClass
import pandas as pd
from datetime import timedelta
import haversine as hs
import tabulate

def query2(q):
    query='select distinct id from user'
    users=q.read_data(query)
    query='select count(id) from trackpoint'
    num_of_track=q.read_data(query)
    query='''SELECT user_id, count(activity.id) FROM activity 
     INNER JOIN trackpoint ON activity.id = trackpoint.activity_id
     group by user_id'''

    data=q.read_data(query)
    maximum=list()
    user=list()
    for d in data:
        maximum.append(d[1])
        user.append(d[0])
    m=max(maximum)
    n=min(maximum)
    print("Average {}".format(num_of_track[0][0]/len(users)))
    print("Maximum Trackpoint user: {0} trackpoints: {1}".format(user[maximum.index(m)], m))
    print("Minimum Trackpoint user: {0} trackpoints: {1}".format(user[maximum.index(n)], n))

def query4(q):
    query="select distinct user_id from activity where transportation_mode='bus'"
    print(q.read_data(query))

def query6(q):
    pass

def query8(q):
    query="SELECT * FROM trackpoint NATURAL JOIN activity"

    tp_data=q.read_data(query)
    users=set()
    time=timedelta(seconds=30)
    print(tp_data[4])
    for r1 in tp_data:
        for r2 in tp_data:
            if r1[7]==r2[7]:
                continue
            if r1[6]==r2[6]+time or r1[6]==r2[6]-time:
                if hs.haversine((r1[2],r1[3]),(r2[2],r2[3]),unit=hs.Unit.METERS)<50:
                    users.add((r1[7],r2[7]))
                    
                
    count=len(users)//2
    print("users those have been close")
    print(users)
    print("#"*20)
    print("Number of users that were close: "+str(count))

"""

"""

def query10(q):
    query=""" WITH distances as 
            (
                SELECT 
                    a.user_id,
                    a.transportation_mode,
                    SUM(ST_Distance_Sphere(POINT(tp1.lon, tp1.lat), POINT(tp2.lon, tp2.lat))) as total_distance,
                    DATE(tp1.date_days) as travel_date
                FROM activity a
                JOIN trackpoint tp1 ON a.id = tp1.activity_id
                JOIN trackpoint tp2 ON a.id = tp2.activity_id AND tp1.id = tp2.id - 1
                WHERE a.transportation_mode IS NOT NULL
                GROUP BY a.user_id, a.transportation_mode, travel_date
            ), ActivityMaxDistance AS (
                SELECT 
                    d.transportation_mode,
                    MAX(d.total_distance) AS max_distance 
                FROM distances d
                GROUP BY d.transportation_mode
            )
            SELECT user_id, d.transportation_mode, max_distance
            FROM distances d
            JOIN ActivityMaxDistance amd ON d.transportation_mode = amd.transportation_mode AND d.total_distance = amd.max_distance
            ORDER BY d.transportation_mode
          """
    data=q.read_data(query)
    print(tabulate.tabulate(data, headers = ['User','Mode','Max Distance']))
""" Output of Query10
  User  Mode          Max Distance
------  --------  ----------------
   128  airplane       1.22664e+07
   068  bike      967084
   128  boat       65554.5
   062  bus            3.42104e+06
   128  car            1.05671e+07
   064  run         1816.17
   128  subway         2.34873e+06
   153  taxi           1.92786e+06
   010  train          8.03732e+06
   153  walk      623411

"""



def query12(q):
    query="""      WITH A AS (
                       SELECT user_id, transportation_mode, COUNT(*) AS num_transports
                       FROM activity
                       WHERE transportation_mode IS NOT NULL
                       GROUP BY user_id, transportation_mode
                   ),
                   B AS (
                       SELECT user_id, MAX(num_transports) AS cnt
                       FROM A
                       GROUP BY user_id
                   )
                   SELECT DISTINCT A.user_id, A.transportation_mode, num_transports
                   FROM A JOIN B ON A.user_id = B.user_id AND A.num_transports = B.cnt
                   ORDER BY A.user_id"""
    data=q.read_data(query)
    print(tabulate.tabulate(data, headers = ['User','Mode','Count']))


""" Output of query 12
  User  Mode      Count
------  ------  -------
   010  walk        153
   020  bike        102
   021  car          10
   052  bus         222
   053  walk         14
   056  bike         22
   058  walk         11
   059  walk          1
   060  walk          2
   062  bus         338
   064  walk         24

"""

    







    
   


q=QueryClass()
print("-"*15+"Query 2")
query2(q)
print("-"*15+"Query 4")
query4(q)
print("-"*15+"Query 8")
query8(q)
print("-"*15+"Query 10")
query10(q)
print("-"*15+"Query 12")
query12(q)







