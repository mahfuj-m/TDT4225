from pprint import pprint
from DbConnector import DbConnector
from datetime import datetime    

# Query 1
# geolife> db.users.countDocuments();
# 182
# geolife> db.activity.countDocuments();
# 14718
# geolife> db.trackpoint.countDocuments();
# 1719034

#Query 2

start = '2010-12-20 00:01:04'
end = '2010-12-20 00:09:47'
time = '2010-12-20 00:06:20'

start_dt = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
end_dt = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
time_dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

print(start_dt <= time_dt <= end_dt)
