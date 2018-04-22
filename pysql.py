import MySQLdb
from urllib import parse


class PySQL:
    """ 
    
    For making Mariadb / Mysql db queries

    """

    FILTER_COMMANDS = {
        "$eq":" = %s ",
        "$in":" IN (%s) ",
        "$nin":" NOT IN (%s) ",
        "$neq":" != %s ",
        "$lt":" < %s ",
        "$lte":" <= %s ",
        "$gt":" > %s ",
        "$gte":" >= %s ",
        "$contains":" LIKE %s ",#like %var%
        "$ncontains":" NOT LIKE %s ",#
        "$null":" IS NULL ", #if 1 else "IS NOT NULL" if 0
        "$sw":" LIKE %s ",#starts with . like %var
        "$ew":" LIKE %s "# endswith like var%
    }

    def __init__(self,user,password,db,host,port):
        self._mysqldb_connection = MySQLdb.connect(user=user,passwd=password,db=db,host=host,port=port)
       

    def commit(self):
        return self._mysqldb_connection.commit()

    def rollback(self):
        return self._mysqldb_connection.rollback()

    def close(self):
        return self._mysqldb_connection.close()


    def execute(self,sql,params=None,many=None,dict_cursor=True):
        #runs the db query . can also be used to run raw queries directly
        """ by default returns cursor object  """

         
        if dict_cursor:
            self.cursor = self._mysqldb_connection.cursor(MySQLdb.cursors.DictCursor)
        else:
            self.cursor = self._mysqldb_connection.cursor()
        
        if many:
            self.cursor.executemany(sql,params)
        else:
            self.cursor.execute(sql,params)

        return self.cursor

    
    
    #PySQL specific method begin from here 

    def __getattr__(self,item):
        self.table_name = item
        self.columns = ['*'] #columns selected for display of records
        self.query_params = []   #for db filtering . parameters entered.
        self.sql = ''
        self.where_sql = ''
        self.join_sql = ''
        self.order_by_sql = ''
        self.group_by_sql = ''


        self.limit_sql = ''


        self.cursor = None
        return self
    
    def __make_table_column(self,column,table_name=None):
        """Example
        Input:  =>  Output:

        users.id  => users.id 
        name      => users.name

        """
        if '.' in column:
            return column
        return "{}.{}".format(table_name,column) if table_name else "{}.{}".format(self.table_name,column)

    def get_columns(self):
        return ','.join([self.__make_table_column(c)  for c in self.columns])
        

    def fields(self,columns):
        #sets columns to select

        """ Example: ['id','name']
        """
        self.columns = columns
        return self


    def fetch(self,limit=None):
        if not self.cursor:
            self.__make_select_sql(limit=limit)

            print (self.sql)
            print (self.query_params)
            self.cursor = self.execute(self.sql,self.query_params)

        results = self.cursor.fetchall()
        self.cursor.close()
        return results

    def fetch_one(self):
        if not self.cursor:
            self.__make_select_sql(limit=None)
            self.cursor = self.execute(self.sql,self.query_params)
            
        result = self.cursor.fetchone()
        self.cursor.close()
        return result
    
    def __set_where(self,where_sql):
        if self.where_sql:
            #check if where starts with AND or OR 
            where_sql = where_sql.strip()
            if where_sql.startswith('OR') or where_sql.startswith("AND"):
                self.where_sql = self.where_sql + " " + where_sql
            else:
                self.where_sql = self.where_sql + " AND " + where_sql
                
            
        else:
            self.where_sql = " WHERE {} ".format(where_sql)

    def __make_sql(self,sql):
        if sql:
            self.sql = self.sql + sql

    def __make_select_sql(self,limit):
        self.sql = "SELECT {} FROM {} ".format(self.get_columns(),self.table_name)

        self.__make_sql(self.join_sql)
        self.__make_sql(self.where_sql)
        self.__make_sql(self.group_by_sql)
        self.__make_sql(self.order_by_sql)
        self.__limit(limit)
       

    def __make_filter(self,k,v):
        #check if val is dict
       
        col = k
        filter_v = None  #the filter value e.g name like '%mosoti%'
        param = v


        print ("Param: ",param, "column:",col)

        if isinstance(param,dict):
          
            filter_v , param = [(k,v) for k,v  in param.items()][0]
        else:
            filter_v = "$eq"
       
        if filter_v == "$null":
         
            if v.get(filter_v) is False:
                filter_v = " IS NOT NULL "
            else:
                filter_v = " IS NULL "
            param = None

        elif filter_v == "$in":
            filter_v = " IN ({}) ".format(','.join(['%s' for i in param]))
        
        elif filter_v == "$nin":
            filter_v = " NOT  IN ({}) ".format(','.join(['%s' for i in param]))
       
        
        else:
            if filter_v == '$contains' or filter_v == "$ncontains":
                param = '%{}%'.format(str(param))
            elif filter_v == "$sw":
                param = '{}%'.format(str(param))

            elif filter_v == "$ew":
                param = '%{}'.format(str(param))

           


            
            filter_v = self.FILTER_COMMANDS.get(filter_v)
        
        return (param,filter_v,)
    

    def __make_or_query_filter(self,data_list):
        qs_l =[]

        for d in data_list:
            for ok,ov in d.items():
                param,filter_v = self.__make_filter(ok,ov)
                self.__build_query_params(param)
                
                            
                q = self.__make_table_column(ok) + filter_v
                qs_l.append(q)

        query = ' OR '.join(qs_l)
        return query


    def __build_query_params(self,param):
        #appends params to existinig
        if param:
            if isinstance(param,list):
                for p in param:
                    self.query_params.append(p)
            else:
                self.query_params.append(param)
                


    def __filter_query(self,filter_data):
        #make filters

        filter_q_l = []

        for k,v in filter_data.items():
            if k == '$or':
                #make for or 
                
                qs_l =self.__make_or_query_filter(filter_data.get('$or'))
                
                
                query = " OR " + qs_l

                filter_q_l.append(query) 

            
            elif k == '$xor':
                    #make for or 
                
                qs_l = self.__make_or_query_filter(filter_data.get('$xor'))

               
                query = " AND ( " + qs_l + " )"
                filter_q_l.append(query) 

            else:
                param,filter_v = self.__make_filter(k,v)
                self.__build_query_params(param)


                q = self.__make_table_column(k) + filter_v

                if len(filter_q_l) == 0:
                    q = q
                else:
                    q = " AND " + q

                filter_q_l.append(q)
        return filter_q_l

    
    def filter(self,filter_data):
    
        """ 
        Filters Requests

        #example full including or

        { "name":{"$contains":"mosoti"},
          "age":{"$lte":30},
          "msisdn":"2541234567",
          "$or":[{ "name":{"$contains":"mogaka"}},
                  {"age":31}
                ],        #this evaluates to => ..  OR name like '%mogaka%'  OR age=31

          "$xor":[{ "name":{"$contains":"mogaka"}},
                   {"age":31}
                 ]  # this evalautes to =>... AND ( name like '%mogaka%'  OR age=31 )
         }

        """

        #reset vals /parameters so that we begin here


        if filter_data:
            filter_q_l = self.__filter_query(filter_data)

            filters_qls = ''.join(filter_q_l).strip()

            if filters_qls.startswith("AND"):
                filters_qls = filters_qls[3:]

            elif filters_qls.startswith("OR"):
                filters_qls = filters_qls[2:]

            self.__set_where(filters_qls)
       
        return self

    def fetch_paginated(self,paginator_obj):
        #receives paginator object

        order_by = paginator_obj.get_order_by()

        filter_data = paginator_obj.get_filter_data()
        page_size = paginator_obj.page_size

        
        self.filter(filter_data)
        self.order_by(order_by)

        results = self.fetch(limit = page_size)
        pagination_data = paginator_obj.get_pagination_data(results)
      
        return {"results":results,"pagination":pagination_data}



    
    def __limit(self,limit):
        if limit:
            self.__build_query_params(limit)
            self.__make_sql(' LIMIT %s ')
       


    def __get_order_by_text(self,val):
        """ Receives string e.g -id or name """

        if val.startswith('-'):
            return "{} DESC".format(self.__make_table_column(val[1:]))
        else:
            return "{} ASC".format(self.__make_table_column(val))



    def order_by(self,order_by_fields):
        """Expects list of fields e.g ['-id','name'] where - is DESC"""

        order_by_sql = ','.join([self.__get_order_by_text(v) for v in order_by_fields])

        if self.order_by_sql:
            self.order_by_sql = self.order_by_sql + ' , ' +  order_by_sql
        else:
            self.order_by_sql = " ORDER BY " + order_by_sql
        return self
    
    def group_by(self,group_by_fields):
        """ Expects fields in list ['id','name'] ... """

        group_by_sql = ','.join([self.__make_table_column(v) for v in group_by_fields])

        if self.group_by_sql:
            self.group_by_sql = self.group_by_sql + group_by_sql
        else:
            self.group_by_sql = " GROUP BY " + group_by_sql
            
        return self

    def __make_join(self,join_type,table_name,condition_data,related_fields):
        """ makes join sql based on type of join and tables """

        on_sql = []

        for k,v in condition_data.items():
            on_sql.append("{} = {} ".format(self.__make_table_column(k),self.__make_table_column(v,table_name)))
        on_sql_str = ' ON {} ' .format(' AND '.join(on_sql))
        join_type_sql = '{} {} '.format(join_type,table_name)

        self.join_sql = self.join_sql + join_type_sql + on_sql_str

        #append the columns to select based on related fields
        if related_fields:
            self.columns.extend([self.__make_table_column(c,table_name) for c in related_fields])




    def inner_join(self,table_name,condition,related_fields=None):
        """ e.g Orders,{"id":"customer_id"}, ['quantity'] 
         This will result to :

         .... Orders.quantity, ....  INNER JOIN Orders ON Customers.id =  Orders.customer_id
        """
        
        self.__make_join('INNER JOIN',table_name,condition,related_fields)
        return self

    def right_join(self,table_name,condition,related_fields=None):
        """ e.g Orders,{"id":"customer_id"}, ['quantity'] 
         This will result to :

         .... Orders.quantity, ....  RIGHT JOIN Orders ON Customers.id =  Orders.customer_id
        """
        
        self.__make_join('RIGHT JOIN',table_name,condition,related_fields)
        return self
            
    def left_join(self,table_name,condition,related_fields=None):
        """ e.g Orders,{"id":"customer_id"}, ['quantity'] 
         This will result to :

         .... Orders.quantity, ....  LEFT JOIN Orders ON Customers.id =  Orders.customer_id
        """
        
        self.__make_join('LEFT JOIN',table_name,condition,related_fields)
        return self

    def update(self,new_data,limit=None):
        """ set this new data as new details
        
        Returns cursor object

        """
       
        col_set = ','.join([" {} = %s ".format(k) for k,v in  new_data.items()])

        filter_params = self.query_params
        self.query_params = []
        update_params = [v for k,v in  new_data.items()]

        update_params.extend(filter_params) #we start with update thn filter

        self.__build_query_params(update_params)


        self.sql = "UPDATE {} SET {} ".format(self.table_name,col_set)
        self.__make_sql(self.where_sql)
        self.__limit(limit)

      
        print(self.query_params)

        print (self.sql)

       
        return self.execute(self.sql,self.query_params)

    def delete(self,limit=None):
        """ Delete with given limit """

        self.sql = "DELETE FROM {}  ".format(self.table_name)
        self.__make_sql(self.where_sql)
        self.__limit(limit)

        print (self.sql)

        return self.execute(self.sql,self.query_params)


    def insert(self,data):
    
        """
         Creates records to db table . Expects a dict of key abd values pair
        
        """

        columns = []

        params = []

        for k,v in data.items():
            columns.append(k)
            params.append(v)
        
        column_placeholders = ','.join(["%s" for v in columns])
        columns = ','.join([v for v in columns])

        self.query_params = params

        self.sql = "INSERT INTO {}({}) VALUES({})".format(self.table_name,columns,column_placeholders)
        
        print (self.sql)
        print (self.query_params)

        return self.execute(self.sql,self.query_params).lastrowid


       



class Paginator:

    def __init__(self,max_page_size=None,url=None,page_number=None,page_size=None,last_seen=None,last_seen_field_name=None,direction=None):
        self.page_number = int(page_number) if page_number else 1
        self.max_page_size = max_page_size if max_page_size else 1000

        if page_size:
            if int(page_size) > self.max_page_size:
                self.page_size = self.max_page_size
            else:
                self.page_size = int(page_size)
        else:
            self.page_size = 25
        self.last_seen_field_name =  last_seen_field_name if last_seen_field_name else 'id'
        self.direction = direction
        self.last_seen = last_seen
        self.url = url

       
        self._where_clause = ''
        self._params = []

       
    def get_order_by(self):
        order_by = []

        if self.page_number == 1 or self.direction == 'next':
            order_by = ["-{}".format(self.last_seen_field_name)] #order descending

        elif self.direction == 'prev':
            order_by = ["{}".format(self.last_seen_field_name)]   #order ascending
        
        return order_by
    
    def get_filter_data(self):
        filter_data = None
        
        if self.page_number == 1:
           filter_data = {}

        elif self.direction == 'prev':
            filter_data = {
                           "{}".format(self.last_seen_field_name):{"$gt":"%s"%(self.last_seen)}
                           }

        elif self.direction == 'next':
            filter_data = {
                           "{}".format(self.last_seen_field_name):{"$lt":"%s"%(self.last_seen)}
                           }
        return filter_data

        
  
    def get_next_link(self,results_list):
        page = self.page_number + 1
        url = self.url


        if len(results_list) < self.page_size:
            return None
            
        if self.direction == 'prev' and  page != 2:
            last_seen_dict = results_list[:-1][0]
        else:
            last_seen_dict = results_list[-1:][0]
            

        url=self.replace_query_param(url, 'page', page)
        url=self.replace_query_param(url, 'dir', 'next')
        url=self.replace_query_param(url, 'last_seen', last_seen_dict.get(self.last_seen_field_name))

        return url


    def get_previous_link(self,results_list):
        page=self.page_number - 1
        url=self.url

        if page == 0:
            return None

        elif len(results_list) == 0:
            #return home link
            url=self.remove_query_param(url, 'page')
            url=self.remove_query_param(url, 'dir')
            url=self.remove_query_param(url, 'last_seen')
            return url
        
        if self.direction == 'next' :
            last_seen_dict = results_list[:-1][0]
        else:
            last_seen_dict = results_list[-1:][0]

        #last_seen_dict = results_list[-1:][0]
        
    
        url=self.replace_query_param(url, 'page', page)
        url=self.replace_query_param(url, 'dir', 'prev')
        url=self.replace_query_param(url, 'last_seen', last_seen_dict.get(self.last_seen_field_name))

        
        return url


    def replace_query_param(self,url, key, val):
        """
        Given a URL and a key/val pair, set or replace an item in the query
        parameters of the URL, and return the new URL.
        """
        (scheme, netloc, path, query, fragment) = parse.urlsplit(url)
        query_dict = parse.parse_qs(query, keep_blank_values=True)
        query_dict[str(key)] = [val]
        query = parse.urlencode(sorted(list(query_dict.items())), doseq=True)
        return parse.urlunsplit((scheme, netloc, path, query, fragment))

    def remove_query_param(self,url, key):
        """
        Given a URL and a key/val pair, remove an item in the query
        parameters of the URL, and return the new URL.
        """
        (scheme, netloc, path, query, fragment) = parse.urlsplit(url)
        query_dict = parse.parse_qs(query, keep_blank_values=True)
        query_dict.pop(key, None)
        query = parse.urlencode(sorted(list(query_dict.items())), doseq=True)
        return parse.urlunsplit((scheme, netloc, path, query, fragment))

   
    def get_pagination_data(self,results_list):
        return {'page_size':self.page_size,
                'next_url': self.get_next_link(results_list),
                'previous_url': self.get_previous_link(results_list)
               }