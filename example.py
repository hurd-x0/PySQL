from pysql import PySQL

from local_settings import DATABASE





db = PySQL(DATABASE.get('USER'),DATABASE.get('PASSWORD'),DATABASE.get('NAME'),DATABASE.get('HOST'),DATABASE.get('PORT'))


print (db)

#all_requests = db.requests.limit(2).fetch_all()

#with_cols  = db.requests.fields(['id','state','access_mode']).limit(2).fetch_all()


filter_data={
    "state":{"$in":['closed','open']},
    #"operator":{"$sw":"v"},
    #"message":{"$contains":"with"},
    
    #"$or":[{"event_id":3},{"event_id":5}]
    #"$xor":[{"event_id":3},{"event_id":{"$gte":5}},{"event_id":{"$null":False}}]
}

filtered_1  = db.requests.fields(['id','state','access_mode','event_id']).filter(filter_data).order_by(['-id','state']).group_by(['message']).fetch(limit=8)

#with joins
filtered_2  = db.requests.fields(['id','state','access_mode','event_id']).inner_join('players',{"msisdn":"phone_number"},related_fields=['country_code']).filter(filter_data).order_by(['-id','state']).group_by(['message']).fetch(4)


#print ([r for r in all_requests])

#print ([r for r in with_cols])

#print ([r for r in filtered_1])
#print ([r for r in filtered_2])


""" Test updates """

filter_data={
    "state":"open",
}
new_data ={
    "state":"closed"
}

c = db.requests.filter(filter_data).update(new_data,limit=5)

db.commit()

print (c.rowcount)


""" Test deletes """

filter_data={
    "state":"closed",
}
c = db.requests.filter(filter_data).delete(limit=5)
db.commit()

print (c.rowcount)



""" TESt insert"""
data={"state":"test","event_id":5,"operator":"mosoticom","msisdn":"0700","service_code":421,"message":"sample",
      "link_id":"1234455","date_created":5434555,"time_created":23456,"country_id":10,"access_mode":"test"
      }

c = db.requests.insert(data)
print (c)

db.commit()
