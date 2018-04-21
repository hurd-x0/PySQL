from pysql import PySQL

DATABASE ={
        'ENGINE': 'mysql',
        'NAME': 'mcheza_bet',
        'USER': 'root',
        'PASSWORD': '2017isgood.r',
        'HOST': '127.0.0.1',
        'PORT': 3306
    }



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

filtered_1  = db.requests.fields(['id','state','access_mode','event_id']).filter(filter_data).order_by(['-id','state']).group_by(['message']).fetch_all()


#print ([r for r in all_requests])

#print ([r for r in with_cols])

print ([r for r in filtered_1])
