import sys,os,sqlite3,string,datetime,random

def databaseMangle(value,choice=string.uppercase):
	'''
	Convert an integer into a sequence of letters.
	'''
	if value is None: return None
	return ''.join([choice[int(digit)] for digit in str(value)])

class databaseMedian:
	def __init__(self):
		self.values = []

	def step(self, value):
		if value is not None: self.values.append(value)

	def finalize(self):
		try:
			theValues = sorted(self.values)
			if len(theValues)==0: return None
			elif len(theValues) % 2 == 1: return theValues[(len(theValues)+1)/2-1]
			else:
				lower = theValues[len(theValues)/2-1]
				upper = theValues[len(theValues)/2]
				return (lower + upper) * 0.5
		except Exception, e: 
			print e
			raise e
			
class databaseMode:
	def __init__(self):
		self.values = []

	def step(self, value):
		if value is not None: self.values.append(value)

	def finalize(self):
		try:
			counts = {}
			for value in self.values:
				try: counts[value] += 1
				except KeyError: counts[value] = 0
			values = sorted(counts,key=counts.__getitem__)
			if len(values)==0: return None
			else: return values[-1]
		except Exception, e: 
			print e,counts
			raise e

class Database(object):

	def __init__(self,path=None):
		if path is None: path = os.getcwd()+"/database.db3"
		self.Connection = sqlite3.connect(path)
		self.Connection.text_factory = str
		self.Cursor = self.Connection.cursor()
		
		self.Connection.create_function('mangle',2,databaseMangle)
		self.Connection.create_aggregate('median',1,databaseMedian)
		self.Connection.create_aggregate('mode',1,databaseMode)
		
		self.Trace = None
		
	def TraceTo(self,file):
		self.Trace = file
		
	def Execute(self,sql,values=None):
		if self.Trace is not None:
			self.Trace.write('%s: %s %s\n'%(datetime.datetime.utcnow(),sql,values))
			self.Trace.flush()
		try:
			if values is None: self.Cursor.execute(sql)
			else: self.Cursor.execute(sql,values)
		except:
			print sql
			raise
		
	def Script(self,sql):
		try: self.Connection.executescript(sql)
		except:
			print sql
			raise
			
	def Commit(self):
		self.Connection.commit()

	def Alter(self,sql):
		'''
		Wrapper around ALTER TABLE statements to handle exceptions if table already has field
		'''
		try: self.Execute(sql)
		except sqlite3.OperationalError,e:
			if 'duplicate column name' in str(e): pass
			else: raise
				
	def Fields(self):
		return [item[0] for item in self.Cursor.description]
				
	def Rows(self,sql,values=None):
		self.Execute(sql,values)
		return self.Cursor.fetchall()
		
	def Row(self,sql,values=None):
		self.Execute(sql,values)
		return self.Cursor.fetchone()
		
	def Values(self,sql,values=None):
		self.Execute(sql,values)
		return [row[0] for row in self.Cursor.fetchall()]
		
	def Value(self,sql,values=None):
		self.Execute(sql,values)
		row = self.Cursor.fetchone()
		if row is None: return None
		else: return row[0]
