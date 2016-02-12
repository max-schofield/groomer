import time

from database import *

class NotDefined: pass
NotDefined = NotDefined()

class Check:
    
        fishing_years = range(1990,2016)
	
	#The database connection
	db = None
	#The dataset (used for some options)
	dataset = None
	
	#Documentation
	brief = None
	desc = None
	#Whether the check should be visible in summaries
	visible = True
	
	#Table and 'shortcuts' for do
	table = None
	clause = None
	column = None
	value = NotDefined
	expr = NotDefined
	
	#A complete listg of error checks
	List = []
	
	@classmethod
	def code(cls):
		return cls.__name__
	
	######################################
			
	def flag(self,table=None,clause=None,details=None):
		if table is None: table = self.table
		if clause is None: clause = self.clause
		assert table is not None and clause is not None
		self.db.Execute('''INSERT INTO checks(code,"table",id,details) SELECT '%s','%s',id, ? FROM %s WHERE %s; '''%(self.code(),self.table,self.table,clause),(details,))
		self.db.Execute('''UPDATE %s SET flags=flags||'%s ' WHERE %s'''%(table,self.code(),clause))
		
	def change(self,table=None,clause=None,column=None,value=NotDefined,expr=NotDefined,details=None):
		if table is None: table = self.table
		if clause is None: clause = self.clause
		if column is None: column = self.column
		assert table is not None and clause is not None and column is not None 
		
		if value is NotDefined: value = self.value
		if expr is NotDefined and self.expr is not NotDefined: expr = self.expr

		if expr is NotDefined:
			self.db.Execute('''INSERT INTO checks(code,"table",column,id,details,orig,new) SELECT '%s','%s','%s',id, ?, %s,? FROM %s WHERE %s; '''%(self.code(),table,column,column,table,clause),(details,value,))
			self.db.Execute('''UPDATE %s SET %s=?, flags=flags||'%s ' WHERE %s'''%(table,column,self.code(),clause),(value,))
		else:
			self.db.Execute('''INSERT INTO checks(code,"table",column,id,details,orig,new) SELECT '%s','%s','%s',id, %s,%s,? FROM %s WHERE %s; '''%(self.code(),table,column,column,expr,table,clause),(details,))
			self.db.Execute('''UPDATE %s SET %s=%s, flags=flags||'%s ' WHERE %s'''%(table,column,expr,self.code(),clause))
		
	def do(self):
		assert self.table is not None and self.clause is not None
		if self.value is not NotDefined: self.change()
		else: self.flag()
			
	def apply(self,force=False):
		print self.code(),
		##Check to see if already done
		count = self.db.Value('''SELECT count(*) FROM checking WHERE code=?''',(self.code(),))
		if count>0 and not force:
			print 'Already done'
			return
		##Not done so do it...
		else:
			start = time.time()
			self.do()
			self.db.Commit()
			elapsed =  round(time.time() - start,1)
			##Record that done
			self.db.Execute('''INSERT INTO checking VALUES(?,datetime('now'));''',(self.code(),))
			print elapsed,'s'
			
	@staticmethod
	def applyAll(checks=None,force=False):
		if checks is None: checks = Check.List
		for check in checks: check().apply(force)


class CHINI(Check):
	'''A special check that does initialisation of tables for checking'''
	visible = False
	
	def do(self):
		##Create table to record what checks are done
		self.db.Execute('''CREATE TABLE IF NOT EXISTS checking (code TEXT,done DATETIME);''')
		self.db.Execute('''DELETE FROM checking;''')
		##Create a table to record details of checks
		self.db.Execute('''CREATE TABLE IF NOT EXISTS checks (code TEXT, "table" TEXT,"column" TEXT, id INTEGER, details TEXT, orig TEXT, new TEXT);''')
		self.db.Execute('''DELETE FROM checks;''')
		
	def apply(self,force=True):
		##Override Check.apply because always need to do this
		self.do()
		
	def report(self):
		return ''
		
class CHSTA(Check):
	'''A special check that does starts the error checks on a table by setting flags to empty string'''
	visible = False
	
	def do(self):
		self.db.Alter('''ALTER TABLE %s ADD COLUMN flags TEXT;'''%self.table)
		self.db.Execute('''UPDATE %s SET flags="";'''%self.table)
		
	def report(self):
		return ''
		


