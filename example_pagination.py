from paginator import Paginator


from pysql import PySQL

from local_settings import DATABASE



db = PySQL(DATABASE.get('USER'),DATABASE.get('PASSWORD'),DATABASE.get('NAME'),DATABASE.get('HOST'),DATABASE.get('PORT'))


p = Paginator(max_page_size=100,url='http://localhost:8000/users?dir=prev&last_seen=48&page=3&page_size=20',page_number=2,page_size=4,
              last_seen=49,direction='prev')


print (p.get_order_by())

print (p.get_filter_data(),p.page_size)

filter_data={
    "state":{"$in":['closed','open']},
    "operator":{"$sw":"s"},
    #"message":{"$contains":"with"},
    
    #"$or":[{"event_id":3},{"event_id":5}]
    #"$xor":[{"event_id":3},{"event_id":{"$gte":5}},{"event_id":{"$null":False}}]
}



#with joins
paginator_obj = p 

data   = db.requests.fields(['id','state','access_mode','event_id']
                              ).inner_join('players',{"msisdn":"phone_number"},
                            related_fields=['country_code']).filter(filter_data).order_by(['state']).fetch_paginated(paginator_obj)

print (data)





