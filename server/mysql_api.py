# this file implements basic layer for communicating with MySQL
import MySQLdb

MYSQL_IP = 'metas.ce0qhjwuid8o.us-west-1.rds.amazonaws.com'
MYSQL_UNAME = 'wubaolin'
MYSQL_PWD = 'wubaolin'
MYSQL_DBNAME = 'metas'


# return db handle after connection to MySQL, 
# following calls will use this client
def connect_to_db():
	db = MySQLdb.connect(MYSQL_IP,MYSQL_UNAME,MYSQL_PWD,MYSQL_DBNAME)
	return db

def close_db(db):
	db.close()
	
# low level functions
def exec_sql_no_data(db,sql_stat):
	cursor = db.cursor()
	cursor.execute(sql_stat)
	db.commit()
	return cursor
	
def exec_sql_data(db,sql_select):
	cursor = db.cursor()
	cursor.execute(sql_select)
	
	datas = cursor.fetchall()

def create_file(file_name):
	global DB
	exec_sql_no_data(DB, "call create_file('" + file_name + "',0);")

DB = connect_to_db()
	
if __name__ == "__main__":
	db = connect_to_db()
	exec_sql_data(db,'select * from abc;')
