# PySQL
Python library for making MySQL and Mariadb  Queries

#Install 

python setup.py install


#How to use:
1.Connect to dbms 

 DATABASE ={
        'NAME': 'db_name',
        'USER': 'db_username',
        'PASSWORD': 'db_password',
        'HOST': '127.0.0.1',
        'PORT': 3306
    }
    
 db = PySQL(DATABASE.get('USER'),DATABASE.get('PASSWORD'),DATABASE.get('NAME'),DATABASE.get('HOST'),DATABASE.get('PORT'))

2. Query table:
 using (1) above , to query table 'users' do the like example below:
 
 
 filter_data={
    "status":{"$in":['closed','open']}
    }
    
 db.users.filter(filter_data).fetch_all(limit=10)
 
 
 3. To paginate results:
 
  paginator_obj = Paginator(max_page_size=100,url='http://localhost:8000/users',page_number=1,page_size=4,
              last_seen=50,direction='prev')
              
  And pass the object to the results method for pagination. 
  i.e 
  
  data   = db.users.fields(['id','status','access_mode']
                              ).inner_join('permissions',{"permission_id":"id"},
                            related_fields=['permission_name']).filter(filter_data).order_by(['status']).fetch_paginated(paginator_obj)



