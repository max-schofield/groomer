import cgi,string

class Tag:
	'''
	Based on http://www.elfsternberg.com/2008/05/30/dwim-html-templating-in-python/
	'''
	def __init__(self, *content, **kw):
		self.data = list(content)
		self._attributes = {}
		for i in kw: self._attributes[(i == 'klass' and 'class') or (i == 'fur' and 'for') or i] = kw[i]
			
	def tag(self):
		return  self.__class__.__name__.lower()

	def render_content(self, content):
		if type(content) == type(""): return content
		if hasattr(content, '__iter__'): return string.join([self.render_content(i) for i in content], '')
		return self.render_content(str(content))

	def __iadd__(self,value):
		self.data.append(value)
		return self

	def __str__(self):
		tagname = self.tag()
		return ('<%s' % tagname +
		       (self._attributes and ' ' or '') +
		       string.join(['%s="%s"' %
				    (a, cgi.escape(str(self._attributes[a])))
				    for a in self._attributes], ' ') +
		       '>' +
		       self.render_content(self.data) +
		       '</%s>\n' % tagname)
		
class Head(Tag): pass
class Body(Tag): pass
class Title(Tag): pass
class Div(Tag): pass
class H1(Tag): pass
class H2(Tag): pass
class H3(Tag): pass
class H4(Tag): pass
class P(Tag): pass
class Br(Tag): pass	
class Img(Tag): pass
class Table(Tag): pass	
class TR(Tag): pass
class TD(Tag): pass	
class TH(Tag): pass		

class Caption(Tag):
	def __init__(self,*args,**kwargs):
		kwargs.update({'klass':'caption'})
		Tag.__init__(self,*args,**kwargs)
	def tag(self):
		return 'p'
class TableCaption(Caption): pass
class FigureCaption(Caption): pass
		
class Html:
	'''
	Use a flushing file so that output is written to disk on the fly
	'''
	stylesheet = '''
		body {
			font: 11pt "Times New Roman",Georgia,serif;
			text-align: justify; 
			width: 16cm;
		}
		p.title, h1, h2, h3, h4 {
			font: 11pt Arial,sans-serif;
			font-weight:bold;
		}
		p.caption {
			font-weight:bold;
			text-align:left;
		}
		table {
			width:95%;
		}
		td {
			font-size: 10pt;
		}
		table .left {
			text-align:left;
		}
		table .right {
			text-align:right;
		}
		table td {
			border: 1px solid;
		}
	'''
	
	def __init__(self,filename,prefix=''):
		self.out = file(filename,'w')
		self += '<html>'
		self += Head('<style type="text/css">',self.stylesheet,'</style>')
		self += '<body>'
		
	def __del__(self):
		self += '</body>'
		self += '</html>'
		
	def __iadd__(self,value):
		self.out.write(str(value))
		self.out.flush()
		return self
		
	
def FARTable(caption,header,rows):
	div = Div(TableCaption(caption),klass='table')
	table = Table()
	tr = TR()
	for index,item in enumerate(header):
		if index==0: klass = 'left'
		else: klass = 'right'
		tr += TD(item,klass=klass)
	table += tr
	if len(rows)>0:
		##Determine the best format for column
		formats = ['%s']*len(rows[0])
		klasses = ['none']*len(rows[0])
		for col in range(len(rows[0])):
			hi = max([row[col] for row in rows])
			if type(hi) is float:
				digits = len(str(int(hi)))
				decimals = 4-digits
				if decimals>0: formats[col] = '%%.%if'%decimals
				else: formats[col] = '%d'
				#klasses[col] = 'right'
			else:
				formats[col] = '%s'
				#klasses[col] = 'left'
			##Text alignment rules for FARs are 1st column left, others right
			klasses[col] = 'left' if col==0 else 'right'
		for row in rows:
			tr = TR()
			for col,value in enumerate(row): 
				format, klass= formats[col],klasses[col]
				if value is None: show = '-'
				else: show = format%value
				tr += TD(show,klass=klass)
			table += tr
	div += table
	return div
	
def FARFigure(filename,caption="No caption defined"):
	return Div(Img(src=filename),FigureCaption(caption),klass='figure')

class FAR(Html):
	
	stylesheet = Html.stylesheet + '''
		body {
			font: 11pt "Times New Roman",Georgia,serif;
			text-align: justify; 
			width: 16cm;
		}
		table td {
			border: none;
		}
	'''

	def __init__(self,filename,prefix=''):
		Html.__init__(self,filename)
		
		self.prefix = prefix
		self.h1 = 0
		self.h2 = 0
		self.h3 = 0
		self.h4 = 0
		self.table = 0
		self.figure = 0
		
	def __number(self,value):
		if isinstance(value,H1):
			self.h1 += 1
			self.h2 = 0
			value.data.insert(0,'%s%i '%(self.prefix,self.h1))
		elif isinstance(value,H2):
			self.h2 += 1
			self.h3 = 0 
			value.data.insert(0,'%s%i.%i. '%(self.prefix,self.h1,self.h2))
		elif isinstance(value,H3):
			self.h3 += 1
			self.h4 = 0 
			value.data.insert(0,'%s%i.%i.%i. '%(self.prefix,self.h1,self.h2,self.h3))
		elif isinstance(value,H4):
			self.h4 += 1
			value.data.insert(0,'%s%i.%i.%i.%i. '%(self.prefix,self.h1,self.h2,self.h3,self.h4))
		elif isinstance(value,TableCaption):
			self.table += 1
			value.data.insert(0,'Table %s%i: '%(self.prefix,self.table))
		elif isinstance(value,FigureCaption):
			self.figure += 1
			value.data.insert(0,'Figure %s%i: '%(self.prefix,self.figure))
		elif isinstance(value,Tag):
			for tag in value.data: self.__number(tag)
			
	def __iadd__(self,value):
		self.__number(value)
		return Html.__iadd__(self,value)
		
	@staticmethod
	def test1():
		far = FAR('test.html',prefix='A')
		far += H1('"1"')
		far += H2('"1.1"')
		far += H3('"1.1.1"')
		far += Figure('First figure')
		far += H1('"2"')
		far += H2('"2.1"')
		far += Figure('Second figure')

if __name__=='__main__':
	FAR.test1()
