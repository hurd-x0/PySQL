import MySQLdb


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
    
    def __make_table_column(self,column):
        """Example
        Input:  =>  Output:

        users.id  => users.id 
        name      => users.name

        """
        if '.' in column:
            return column
        return "{}.{}".format(self.table_name,column)

    def get_columns(self):
        return ','.join([self.__make_table_column(c)  for c in self.columns])
        

    def fields(self,columns):
        #sets columns to select

        """ Example: ['id','name']
        """
        self.columns = columns
        return self


    def fetch_all(self):
        if not self.cursor:
            self.__make_select_sql()
            print (self.sql)
            print (self.query_params)


            self.cursor = self.execute(self.sql,self.query_params)

        results = self.cursor.fetchall()
        self.cursor.close()
        return results

    def fetch_one(self):
        if not self.cursor:
            self.__make_select_sql()
            self.cursor = self.execute(self.sql,self.query_params)
            
        result = self.cursor.fetchone()
        self.cursor.close()
        return result
    
    def __set_where(self,where_sql):
        if self.where_sql:
            self.where_sql = self.where_sql + " " + where_sql
        else:
            self.where_sql = " WHERE {} ".format(where_sql)

    def __make_sql(self,sql):
        if sql:
            self.sql = self.sql + sql

    def __make_select_sql(self):
        self.sql = "SELECT {} FROM {} ".format(self.get_columns(),self.table_name)

        self.__make_sql(self.join_sql)
        self.__make_sql(self.where_sql)
        self.__make_sql(self.group_by_sql)
        self.__make_sql(self.order_by_sql)
        self.__make_sql(self.limit_sql)

       


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
        Filters requests

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
    
    def limit(self,limit):
        self.query_params.append(limit)
        self.limit_sql = ' LIMIT %s '
        return self


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
            self.order_by_sql = self.order_by_sql + order_by_sql
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



