# Transaction Manager
import thread
import threading
import time
import file_handler

# map transaction id => []
Transactions = {}
current_min_transaction_id = 1000
mutex = threading.Lock()

# multitreading, handle trasaction fails
def timer_tick_function():
	global Transactions,mutex
	while True:	
		cur_time = time.time()
		LockResource()
		for k in Transactions.keys():
			if Transactions[k] == None:
				continue
			if cur_time - Transactions[k][-1] > 15.0:
				file_handler.abort_transaction(k,Transactions[k])			
				Transactions[k] = None
				
		UnlockResource()
		time.sleep(2)
	
def LockResource():
	mutex.acquire()
	
def UnlockResource():
	mutex.release()
	
def Init():
	Transactions = {}
	thread.start_new_thread(timer_tick_function,())	

def has_key(key):
	return Transactions.has_key(key)
	
def get_key(key):
	return Transactions[key]

def get_next_trans_id():
	global current_min_transaction_id
	tmp = current_min_transaction_id
	current_min_transaction_id += 1
	return tmp
	
def AddTrans(key,value):
	global Transactions
	Transactions[key] = value

def DelTrans(key):
	global Transactions
	if Transactions.has_key(key) == False:
		return
	Transactions[key] = None