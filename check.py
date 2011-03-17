import rpy2.rpy_classic as rpy
rpy.set_default_mode(rpy.NO_CONVERSION)
from rpy2.rpy_classic import r as R

import math,time

from database import *
from html import *

class NotDefined: pass
NotDefined = NotDefined()

class Check:
	
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
		
	def change(self,table=None,clause=None,column=None,value=NotDefined,expr=None,details=None):
		if table is None: table = self.table
		if clause is None: clause = self.clause
		if column is None: column = self.column
		if value is NotDefined: value = self.value
		assert table is not None and clause is not None and column is not None 
		if expr is None:
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
		
	######################################
		
	def filename(self,filename):
		'''Create a valid filename from a string'''
		return str(filename.replace(' ','_').replace('/','di').replace('>','gt').replace('>=','ge').replace('<','lt').replace('<=','le').replace('"','').replace("'",''))
		
	def scatterplot(self,table,x,y,where='',transform='',xlab='',ylab='',lines=[],caption='No caption defined',alpha=0.1):
		sql = '''SELECT %s,%s FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL'''%(x,y,table,x,y)
		wheres = []
		if 'log' in transform: wheres.append('''%s>0 AND %s>0'''%(x,y))
		if len(where)>0: wheres.append(where)
		if len(wheres)>0: sql += ''' AND ''' + ''' AND '''.join(wheres)
		rows = self.db.Rows(sql)
			
		filename = 'summary/'+self.filename('%s %s %s %s %s scat.png'%(table,x,y,transform,where))
		
		if transform=='log10': func = math.log10
		else: func = lambda x: x
		rows = [(func(x),func(y)) for x,y in rows]

		if xlab=='': xlab = x
		if ylab=='': ylab = y
		if transform!='': 
			xlab = '%s (%s)'%(transform,xlab)
			ylab = '%s (%s)'%(transform,ylab)
		R.png(filename,600,400)
		if len(rows)>0:
			R.plot([x for x,y in rows],[y for x,y in rows],xlab=xlab,ylab=ylab,pch=16,col=R.rgb(0,0,0,alpha))
			for line in lines: R.abline(a=line[0],b=line[1],lty=2)
		R.dev_off()
		
		return FARFigure(filename,caption)	
		
	def quantiles(self,values,at=[0.05,0.5,0.95]):
		'''Calculate quantiles of a vector'''
		qs = []
		for p in at: 
			index = int(len(values)*p)-1
			qs.append(values[index])
		return qs

	def histogram(self,table,field,where='',transform='',xlab='',ylab='',lines=[],caption='No caption defined'):
		'''Create a histogram for a field in a table'''
		sql = '''SELECT %s FROM %s  WHERE %s IS NOT NULL'''%(field,table,field)
		wheres = []
		if 'log' in transform: wheres.append('''%s>0'''%field)
		if len(where)>0: wheres.append(where)
		if len(wheres)>0: sql += ''' AND ''' + ''' AND '''.join(wheres)
		values = self.db.Values(sql)
		values.sort()
		n = len(values)
		
		if n<10: return P('Only %s values, not plotting a histogram'%n)
			
		p1,p5,median,p95,p99 = self.quantiles(values,at=[0.01,0.05,0.5,0.95,0.99])
		geomean = math.exp(sum([math.log(value) for value in values if value>0])/len(values))
		#print p1,p5,median,p95,p99,geomean
		
		if transform=='log10': func = math.log10
		else: func = lambda x: x
		values = [func(value) for value in values if value>=p1 and value<=p99]
		lines = [func(line) for line in lines]
		
		if len(values)<10: return P('Only %s values, not plotting a histogram'%len(values))

		if xlab=='': xlab = field
		if transform!='': xlab = '%s (%s)'%(transform,xlab)
		if ylab=='': ylab = 'Records'
			
		filename = 'summary/'+self.filename('%s %s %s hist.png'%(table,field,where))
		R.png(filename,400,300)
		R.hist(values,breaks=30,main='',xlab=xlab,ylab=ylab,col='grey')
		R.legend("topright",legend=['N=%i'%n,'P5=%.2f'%p5,'Med=%.2f'%median,'GM=%.2f'%geomean,'P95=%.2f'%p95],bty='n')
		for line in lines: R.abline(v=line,lty=2)
		R.dev_off()
		
		caption += '''
		N: number of fishing events; P5: 5th percentile; Med: median; GM: geometrc mean; P95: 95th percentile
		'''
		
		return FARFigure(filename,caption)
	
	def summarise(self):
		'''A default summary of the check'''
		div = Div()
		count = self.db.Value('''SELECT count(*) FROM checks WHERE code='%s';'''%(self.code()))
		if count==0: div += P('No records where flagged by this check.')
		else:
			##A table by details, old, new if appropriate
			table = self.db.Rows('''SELECT details,orig,new,count(*) FROM checks WHERE code='%s' GROUP BY details,orig,new;'''%(self.code()))
			if len(table)==1: div += P('A total of %i records were flagged by this check.'%table[0][3])
			else: div += FARTable('Summary of number of records',('Details','Original value','New value','Records'),table)
				
		return div
		
	def report(self):
		return Div(
			H2('%s (%s)'%(self.brief,self.code())),
			P(self.desc),
			self.summarise()
		)
		
	def view(self):
		html = FAR('temp.html')
		html += self.report()
		del html
		os.system('firefox temp.html')
		
	@staticmethod
	def viewAll(checks=None):
		if checks is None: checks = Check.List
		html = FAR('temp.html')
		for check in checks: html += check().report()
		del html
		os.system('firefox temp.html')
		
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
		


