import sys,os,sqlite3,string,datetime,random

version = 1.0

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
			if len(theValues) % 2 == 1: return theValues[(len(theValues)+1)/2-1]
			else:
				lower = theValues[len(theValues)/2-1]
				upper = theValues[len(theValues)/2]
				return (lower + upper) * 0.5
		except Exception, e: 
			print e
			raise e

class Database(object):

	def __init__(self,path=None):
		if path is None: path = os.getcwd()+"/database.db3"
		self.Connection = sqlite3.connect(path)
		self.Connection.text_factory = str
		self.Cursor = self.Connection.cursor()
		
		self.Connection.create_function('mangle',2,databaseMangle)
		self.Connection.create_aggregate('median',1,databaseMedian)
		
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

class Dataset:

	def __init__(self,**kwargs):

		##Set some defaults
		for kw,arg in {
			'request_extra_notes':'', ##Any extra notes to insert into email
			
			'extract':'extract', ##The name of the directory holding the dataextract
			'extract_datetime_format':1, ##Flag for one of the datetime formats used. See the load method for how this is used
			
			'load_qmastats':'RIB',##Ribaldo (RIB) uses the general FMA boundaries for QMAs, so insert stats for this species. Usseful to override for FLA species codes eg SFL.
			
			'groom_reset':True, ##Reset grooming flags to NULL?
			'groom_trip_details':True,
			'groom_estimated_subcatch':True,
			'groom_estimated_subcatch_catch':None,
			'groom_landing':True,
			'groom_fishing_event':True,
			'groom_excludes':[],
			'groom_GRR_species_min': 100,
			'groom_fishstock_stat':False,
			
			'allocate_straddles_drop':True,
			'allocate_fishstocks':None,
			
			'extract_char_catches':'prop',
			'extract_cpue_catches':['est','prop'], ##More than one field can be extracted to cpue.txt file
			
			'zones':None,
			
			'scale_to_totals': True,
			
			'extra_fields':[], ##Additional field to carry over from fishing_event to simple.db3 and cpue.txt
			'extra_factors':[] ##Additonal factors for Tanga:Web schema  e.g. [('port_stat',0,1)]
			
		}.items(): setattr(self,kw,arg)

		##Apply overrides specified in arguments
		for kw,arg in kwargs.items(): setattr(self,kw,arg)
			
		##Give a name based on name of current working directory
		self.name = os.getcwd().split('/')[-1]
		
		##Create database connection
		self.db = Database('database.db3')
			
	def prerequest(self):
		'''Automatically create an email and SQL for a prerequest (provides a summary of the 
		methods, targets and areas where most of these species were caught).'''

		species = ','.join(self.species)
		species_quoted = ','.join([repr(item) for item in self.species])
		fishstocks_list = []
		for fishstocks in self.fishstocks.values(): fishstocks_list.extend(fishstocks)
		fishstocks  = ','.join(fishstocks_list)
		fishstocks_quoted = ','.join([repr(item) for item in fishstocks_list])

		email = '''
		Dear RDM,

		Trophia Ltd has been contracted by %s under project "%s" to analyse catch and effort data for %s. We would like
		to do an initial summary of the data for these Fishstocks so that we can determine the best set of criteria for our data extract.
		Please could you do a count of the number of fishing_events recording each primary_method, target_species, start_stats_area_code
		and form_type for all of the trips that landed to these Fishstocks between %s and %s.'''%(self.client,self.project,fishstocks,self.begin,self.end)

		email += '''
		\n\nAppended to the end of this message is draft SQL that should do the summaries required.  This SQL is
		based on the SQL that we have received from MFish in previous extracts. It involves the creation of a temporary table which lists the qualifying trips
		and extracts of various data based on this table. Could you please supply us with the final SQL used, so that for future extracts we can
		correct any errors or omissions that we have made.

		The request is subject to the conditions of the existing confidentiality agreement between MFish and Trophia.
		Please contact me by email if you require any further details to support this request.
		'''

		sql = ''

		sql += '''
		/* *********************************************/
		/*    List of qualifying trips                                                */
		/* *********************************************/
		/* Create temporary table with index on trip */
		use tempdb;
		go
		create table tempdb..trophia_%s (trip keys null);
		go
		create index index_1 on tempdb..trophia_%s (trip);
		go
		'''%(self.name,self.name)

		sql += '''
		/* Insert trips that meet landings criteria*/
		insert tempdb..trophia_%s
		select distinct la.trip
		from warehou..ce_landing la
		where la.interp_yn = 'Y'
		and la.landing_datetime >= '%s'
		and la.landing_datetime < '%s' 
		and la.species_code in (%s)'''%(self.name,self.begin,self.end,species_quoted)
		if len(fishstocks_list)>0: sql += '''\nand la.fishstock_code in (%s)'''%fishstocks_quoted
		sql += ''';\ngo\n'''

		for factor in ('primary_method','target_species', 'start_stats_area_code','form_type'):
			sql += '''
		/* *********************************************/
		/*   Fishing events by %s for trips selected                         */
		/* *********************************************/
		select 
		  fi.%s,
		  count(*),
		  sum(fi.effort_num),
		  sum(fi.catch_weight)
		from warehou..ce_fishing_event fi,tempdb..trophia_%s tt
		where fi.interp_yn = 'Y' and tt.trip = fi.trip
		group by fi.%s;
			'''%(factor,factor,self.name,factor)

		file('prerequest.txt','w').write(email+'\n\n'+sql+'\n\n')

	def request(self):
		'''Automatically create a data request email based'''
		
		species = ','.join(self.species)
		species_quoted = ','.join([repr(item) for item in self.species])
		fishstocks_list = []
		for fishstocks in self.fishstocks.values(): fishstocks_list.extend(fishstocks)
		fishstocks  = ','.join(fishstocks_list)
		fishstocks_quoted = ','.join([repr(item) for item in fishstocks_list])

		##Email text
		email = '''
		Dear RDM,

		Trophia Ltd has been contracted by %s under project "%s" to analyse catch and effort data for %s. We would like
		to obtain catch and effort data for fishing trips that occurred between %s and %s, and that:'''%(self.client,self.project,fishstocks,self.begin,self.end)

		if len(fishstocks_list)>0:  email += '''\n  * landed to %s\n'''%(fishstocks)

		criteria = []
		if len(self.statareas): criteria.append('''- were in statistical area(s) %s,'''%(','.join(self.statareas)))
		if len(self.methods): criteria.append('''- used method(s) %s,'''%(','.join(self.methods)))
		if len(self.targets): criteria.append('''- targeted species %s'''%(','.join(self.targets)))
		if hasattr(self,'targets_not') and len(self.targets_not): criteria.append('''- did not target species %s'''%(','.join(self.targets_not)))
		if len(criteria)>0:
			email += '''    OR\n    * had fishing events that:\n\t\t'''
			email += '\n    AND\n    '.join(criteria)

		email += '''\n\nFor these trips we would like to obtain all effort data as well as landings and estimated catch data for the following species : %s'''%species

		email += '''\n\nIn addition, we would like to obtain monthly harvest return (MHR) data for all available months for the above fishstocks
		by month, client and Fishstock.

		Attached to this email is draft SQL that includes the fields required for this extract. It involves the creation of a temporary table which lists the qualifying trips
		and extracts of various data based on this table. Could you please supply us with the final SQL used, so that for future extracts we can
		correct any errors or omissions that we have made. Please could you provide the data in the usual MFish fixed field width text format.

		%s

		The request is subject to the conditions of the existing confidentiality agreement between MFish and Trophia.
		Please contact me by email if you require any further details to support this request.
		'''%self.request_extra_notes
	
		##Separate files for each extract file
		tt = '''
		/* *********************************************/
		/*    List of trips that define the extract         */
		/* *********************************************/
		/* Create temporary table with index on trip */
		use tempdb
		go
		
		create table tempdb..trophia_%s (trip keys null)
		go
		
		create index index_1 on tempdb..trophia_%s (trip)
		go
		'''%(self.name,self.name)

		tt += '''
		/* Insert trips that meet landings criteria*/
		insert tempdb..trophia_%s
		select distinct la.trip
		from warehou..ce_landing la
		where la.interp_yn = 'Y'
		  and la.landing_datetime >= '%s'
		  and la.landing_datetime < '%s' '''%(self.name,self.begin,self.end)
		if len(fishstocks_list)>0: tt += '''\n  and la.fishstock_code in (%s)'''%fishstocks_quoted
		else: tt += '''\n  and la.species_code in (%s)'''%species_quoted

		tt += '''\ngo\n'''

		if len(criteria)>0:
			tt += '''
		/* Insert trips that meet effort criteria*/
		insert tempdb..trophia_%s
		select distinct fi.trip
		from warehou..ce_fishing_event fi  
		where fi.interp_yn = 'Y'
		  and fi.trip not in (select trip from tempdb..trophia_%s)
		  and fi.start_datetime >= '%s'
		  and fi.start_datetime < '%s' '''%(self.name,self.name,self.begin,self.end)

			if len(self.statareas)>0: tt += '''\n  and fi.start_stats_area_code in (%s)'''%','.join([repr(item) for item in self.statareas])
			if len(self.methods)>0: tt += '''\n  and fi.primary_method in (%s)'''%','.join([repr(item) for item in self.methods])
			if len(self.targets)>0: tt += '''\n  and fi.target_species in (%s)'''%','.join([repr(item) for item in self.targets])
			if hasattr(self,'targets_not'): tt += '''\n  and fi.target_species not in (%s)'''%','.join([repr(item) for item in self.targets_not])
				
			tt += '''\ngo\n'''

		td = '''
		/* *********************************************/
		/*      Trip details for trips selected     */
		/* *********************************************/
		select
		  td.trip,
		  td.trip_version,
		  td.start_datetime,
		  td.end_datetime,
		  td.vessel_key,
		  td.client_key
		from warehou..ce_trip_details td,tempdb..trophia_%s tt
		where tt.trip = td.trip
		go
		'''%self.name

		fi = '''
		/* *********************************************/
		/*      Fishing events for trips selected     */
		/* *********************************************/
		select 
		  fi.event_key,
		  fi.version_seqno,
		  fi.group_key,
		  fi.start_datetime,
		  fi.end_datetime,
		  fi.primary_method,
		  fi.target_species,
		  fi.  fishing_duration,
		  fi.catch_weight,
		  fi.catch_weight_other,
		  fi.non_  fish_yn,
		  fi.effort_depth,
		  fi.effort_height,
		  fi.effort_num,
		  fi.effort_num_2,
		  fi.effort_seqno,
		  fi.effort_total_num,
		  fi.effort_width,
		  fi.effort_length,
		  fi.effort_speed,
		  fi.surface_temp,
		  fi.total_hook_num,
		  fi.set_end_datetime,
		  fi.haul_start_datetime,
		  fi.haul_start_wind_speed,
		  fi.haul_end_wind_speed,
		  fi.set_start_wind_speed,
		  fi.set_start_wind_direction,
		  fi.haul_end_wind_direction,
		  fi.haul_end_surface_temp,
		  fi.float_num,
		  fi.light_stick_num,
		  fi.line_shooter_yn,
		  fi.condition_type,
		  fi.total_net_length,
		  fi.double_reel_num,
		  fi.pair_trawl_yn,
		  fi.bottom_depth,
		  fi.trunc_start_lat,
		  fi.trunc_start_long,
		  fi.trunc_end_lat,
		  fi.trunc_end_long,
		  fi.start_stats_area_code,
		  fi.vessel_key,
		  fi.client_key,
		  fi.dcf_key ,
		  fi.form_type,
		  fi.trip
		from warehou..ce_fishing_event fi,tempdb..trophia_%s tt
		where fi.interp_yn = 'Y' and tt.trip = fi.trip
		go
		'''%(self.name)

		ca = '''
		/* *********************************************/
		/*      Estimated catch for species of interest     */
		/* *********************************************/
		select 
		  ca.event_key,
		  ca.version_seqno,
		  ca.group_key,
		  ca.species_code,
		  ca.  catch_weight,
		  ca.trip
		from warehou..ce_estimated_subcatch ca,tempdb..trophia_%s tt
		where ca.interp_yn = 'Y'
		  and tt.trip = ca.trip
		  and and ca.species_code in (%s)
		go
		'''%(self.name,species_quoted)
			
		pr = '''
		/* *********************************************/
		/*      Processed catch for species of interest     */
		/* *********************************************/
		select 
		  pr.event_key,
		  pr.version_seqno,
		  pr.group_key,
		  pr.spec  prod_seqno,
		  pr.spec  prod_action_type
		  pr.  processed_datetime,
		  pr.species_code,
		  pr.state_code,
		  pr.unit_type,
		  pr.unit_num,
		  pr.unit_weight,
		  pr.conv_factor,
		  pr.  processed_weight,
		  pr.  processed_weight_type,
		  pr.green_weight,
		  pr.green_weight_type,
		  pr.dcf_key,
		  pr.form_type,
		  pr.trip
		from warehou..ce_processed_catch pr,tempdb..trophia_%s tt
		where pr.interp_yn = 'Y'
		  and tt.trip = pr.trip
		  and pr.species_code in (%s)
		go
		'''%(self.name,species_quoted)
			
		la = '''
		/* *********************************************/
		/*      Landings for the species of interest     */
		/* *********************************************/
		select 
		  la.event_key,
		  la.version_seqno,
		  la.group_key,
		  la.specprod_seqno,
		  la.  landing_datetime,
		  la.  landing_name,
		  la.species_code,
		  la.fishstock_code,
		  la.state_code,
		  la.destination_type,
		  la.unit_type,
		  la.unit_num,
		  la.unit_num_  latest,
		  la.unit_num_other,
		  la.unit_weight,
		  la.conv_factor,
		  la.green_weight,
		  la.green_weight_type,
		  la.processed_weight,
		  la.processed_weight_type,
		  la.tranship_vessel_key,
		  la.vessel_key,
		  la.client_key,
		  la.dcf_key,
		  la.form_type,
		  la.trip
		from warehou..ce_landing la, tempdb..trophia_%s tt
		where la.interp_yn = 'Y'
		  and tt.trip = la.trip
		  and la.species_code in (%s)
		go
		'''%(self.name,species_quoted)

		vs = '''
		/* *********************************************/
		/*      Vessel histories for any vessels in the selected trips    */
		/* *********************************************/
		select 
		  vs.history_start_datetime,
		  vs.history_end_datetime,
		  vs.vessel_key,
		  vs.flag_nationality_code,
		  vs.overall_length_metres,
		  vs.registered_length_metres,
		  vs.draught_metres,
		  vs.beam_metres,
		  vs.gross_tonnes,
		  vs.max_speed_knots,
		  vs.service_speed_knots,
		  vs.engine_kilowatts,
		  vs.max_duration_days,
		  vs.built_year,
		  vs.tenders_number,
		  vs.total_crew_number,
		  vs.base_region_code,
		  vs.base_port_code
		from corporat..vs_vessel_history vs, tempdb..trophia_%s tt
		where vs.history_end_datetime >= '%s'
		  and vs.history_start_datetime < '%s'
		  and vs.vessel_key in (select distinct vessel_key
		  from warehou..ce_trip_details td,tempdb..trophia_%s tt
		  where tt.trip = td.trip)
		go
		'''%(self.name,self.begin,self.end,self.name)

		mq = '''
		/* *********************************************/
		/*     MHR data    */
		/* *********************************************/
		select mq.perorg_key,
		  mq.month,
		  mq.year,
		  mq.stock_code,
		  mq.quantity
		from warehou..mh_actual_mhr_quantity mq
		where mq.from_datetime >= '01 Oct 2001'
		  and mq.from_datetime < getdate()
		  and mq.stock_code in (%s)
		go
		'''%fishstocks_quoted

		if not os.path.exists('request'): os.mkdir('request')
		for item in ('email','tt','td','fi','ca','pr','la','vs','mq'):
			content = locals()[item]
			##Replace unecessary indents caused by Python indenting above
			content = content.replace('\n\t\t','\n')
			##Add a header
			content = '''/* Generated for "%s" by %s (v%s) at %s UTC */\n'''%(self.name,__file__,version,datetime.datetime.utcnow())+content
			##Decide on extension
			ext = 'txt' if item=='email' else 'sql'
			##Write to file
			file('request/trophia_%s_%s.%s'%(self.name,item,ext),'w').write(content)

	def loadFile(self,table,filetags=None,filename=None,format=None):
		'''Function for reading a file into a table'''
		self.db.Execute('''DELETE FROM %s;'''%table)
		
		if not filename:
			##Assume that loooking for table name in filename unless filetag in specified
			if filetags is None: filetags = []
			filetags = [table] + filetags
				
			##Find the appropriate file
			filenames = []
			for filename in os.listdir(self.extract):
				for filetag in filetags:
					if filetag in filename.lower() and '.sql' not in filename: filenames.append(filename)
			if len(filenames)<1: raise Exception('File not found for "%s" using tags %s'%(table,filetags))
			if len(filenames)>1: raise Exception('More than one matching filename: %s'%filenames)
			filename = self.extract + '/'+ filenames[0]
		
		data = file(filename)
		
		##Determine file format from extension
		if format is None:
			if ".tab" in filename: format = "\t"
			elif ".csv" in filename: format = ","
			else: format = 'fixed'
		
		if format=='fixed':
			##Read in the column headers
			names = data.readline().strip().split()
			##Read in the dashes and determine column positions from them
			##The previous method used assumed a single space between each set of dashes
			##but that is not always the case.  This method actually determines the start and end of each set of dashes
			dashes = data.readline()
			characters = []
			end = 0
			while True:
				start = dashes.find('-',end)
				if start<0: break
				end = dashes.find(' ',start)
				characters.append((start,end))
		else:
			##Read in the column headers
			names = data.readline().strip().split(format)
			
		print filename,":",names
			
		##Read in each line, insert a comma at each point and then write it out
		sql = '''INSERT INTO %s(%s) VALUES(%s);'''%(table,','.join(names),','.join(['?']*len(names)))
		values = []
		count = 0
		blank_lines = 0
		
		while True:
			line = str(data.readline()) ##Convert everything to plain text string
			if format=='fixed':
				if len(line.strip())<1: 
					blank_lines += 1
					if blank_lines==1: continue ##The blank line at the end of the data
					else: break ##Escape if the row with number of rows (see below) is not present. Sometimes it is not.
				elif line[0]=='(':
					##The last line ( which starts with a '(' ) gives the number of rows that 
					##are meant to be in the data. Check this with count of row.
					if str(count) not in line: raise Exception('Rows counted (%s) does not match "%s"'%(count,line))
					break
			else:
				if len(line)==0: break
					
			if format=='fixed':
				fields = []
				for index,(start,end) in enumerate(characters): fields.append(line[start:end].strip())
			else: fields = line.split(format)
				
			cleaned = [None]*len(fields)
			for index,value in enumerate(fields):
				value = value.strip() ##This is necessary to remove line ends. It can not be done before here because for tab/space delimited files stripping will remove those delimiters.
				if value == 'NULL' or value=='': value = None
				elif 'datetime' in names[index]: 
					if self.extract_datetime_format==1:
						value = datetime.datetime.strptime(value,'%b %d %Y %I:%M%p')
					else:
						##Deal with datetime format = 20/02/1991 12:00:00.000 AM
						bits = value.split()
						if len(bits)>0: value = datetime.datetime.strptime(bits[0],'%d/%m/%Y')
						else: value = None
				cleaned[index] = value
				
			values.append(cleaned)
			count += 1
		
		self.db.Cursor.executemany(sql,values)
		self.db.Commit()

	def load(self):
		##Delete the existing database
		if os.path.exists('database.db3'): os.remove('database.db3')
			
		##Create a new db connection and initialise it
		self.db = Database('database.db3')
		
		self.db.Script(file('/Trophia/Tanga/Groomer/tangagroomer/dataset.sql').read())
			
		self.loadFile('trip_details')
		self.loadFile('fishing_event',['effort'])
		self.loadFile('estimated_subcatch',['estimated_catch','est_catch','estcatch'])
		self.loadFile('processed_catch',['processed catch','proc_catch','procatch'])
		self.loadFile('landing',['landed_catch'])
		self.loadFile('vessel_history',['vessel_specs','vessel_spexs','vessel_specx'])
		self.loadFile('mhr')

		self.loadFile('qmr',filename='/Trophia/Tanga/Data/shared/QMR.txt')
		self.loadFile('dqss',filename='/Trophia/Tanga/Data/shared/DQSS_range_checks.txt',format='\t')
		self.loadFile('qmastats',filename='/Trophia/Tanga/Data/shared/qmastats.txt',format=",")

		try: self.loadFile('history',filename='history.csv')
		except Exception, e: print e,'Ignoring'

		for species in self.species:
			if self.db.Value('''SELECT count(*) FROM qmastats WHERE species=='%s';'''%species)==0:
				print 'No stats in qmastats for species %s. Using stats for "%s".'%(species,definition.load_qmastats)
				self.db.Execute('''INSERT INTO qmastats(species,qma,stat) SELECT '%s','%s'||(CASE length(qma)>4 WHEN 1 THEN substr(qma,4,2) ELSE substr(qma,4,1) END),stat FROM qmastats WHERE species=='%s';'''%(species,species,definition.load_qmastats))

		self.db.Execute('''INSERT INTO status(task,done) VALUES('load',datetime('now'));''')
		self.db.Commit()
	
	def groom(self):

		lastValidDate = datetime.datetime.strptime(self.end,'%d %b %Y').strftime('%Y-%m-%d')

		self.db.Execute('''CREATE TABLE IF NOT EXISTS checks (done DATETIME,"table" TEXT, code TEXT, action TEXT, field TEXT, new TEXT,clause TEXT);''')
		self.db.Execute('''DELETE FROM checks;''')
		def Check(table,code,action,field='NULL',to='NULL',clause='NULL'):
			self.db.Execute('''INSERT INTO checks VALUES(datetime('now'),'%(table)s','%(code)s','%(action)s','%(field)s',"%(to)s","%(clause)s");'''%vars())
			
		def Init(table):
			self.db.Alter('''ALTER TABLE %s ADD COLUMN changed TEXT;'''%table)
			self.db.Alter('''ALTER TABLE %s ADD COLUMN dropped TEXT;'''%table)
			if self.groom_reset: 
				self.db.Execute('''UPDATE %s SET changed=NULL,dropped=NULL;'''%table)
			self.db.Execute('''CREATE TABLE IF NOT EXISTS %s_changes (id INTEGER, code TEXT, field TEXT, orig TEXT, new TEXT);'''%table)
			if self.groom_reset: 
				for row in self.db.Rows('''SELECT field,orig,id FROM %s_changes;'''%table): self.db.Execute('''UPDATE %s SET %s=? WHERE id=?'''%(table,row[0]),(row[1],row[2]))
				self.db.Execute('''DELETE FROM %s_changes;'''%table)
			
		def Change(table,code,field,to,where):
			'''
			Make a change to data and record that change in the '<table>_changes' table
			'''
			self.db.Execute('''INSERT INTO %(table)s_changes SELECT id,'%(code)s','%(field)s',%(field)s,%(to)s FROM %(table)s WHERE %(where)s; '''%vars())
			self.db.Execute('''UPDATE %(table)s SET %(field)s=%(to)s,changed='%(code)s' WHERE %(where)s;'''%vars())
			Check(table,code,'change',field,to,where)
			
		def Drop(table,code,where):
			'''
			Drop rows from a table by marking column dropped
			'''
			self.db.Execute('''UPDATE %(table)s SET dropped='%(code)s' WHERE dropped IS NULL AND %(where)s;'''%vars())
			Check(table,code,'drop',field='',to='NULL',clause=where)
			
		if self.groom_trip_details: 
			Init('trip_details')
			
			## TRD (Starr D.1.8): "Calculate "best date" for trip"
			##Start by adding landing_datetime to make calcs easier
			self.db.Alter('''ALTER TABLE trip_details ADD COLUMN landing_datetime DATETIME;''')
			self.db.Execute('''UPDATE trip_details SET landing_datetime=(SELECT landing.landing_datetime FROM trip_details LEFT JOIN landing ON trip_details.trip=landing.trip)''')
			##.1
			self.db.Alter('''ALTER TABLE trip_details ADD COLUMN trip_length INTEGER;''')
			self.db.Execute('''UPDATE trip_details SET trip_length=julianday(end_datetime)-julianday(start_datetime);''')
			self.db.Alter('''ALTER TABLE trip_details ADD COLUMN trip_length_alt INTEGER;''')
			self.db.Execute('''UPDATE trip_details SET trip_length_alt=julianday(end_datetime)-julianday(landing_datetime) WHERE landing_datetime<end_datetime;''')
			
			self.db.Execute('''DROP TABLE IF EXISTS trip_lengths;''')
			self.db.Execute('''CREATE TABLE trip_lengths(form_type TEXT,count INTEGER,p95 REAL);''')
			cutoffs = {}
			for form_type,count in self.db.Rows('''SELECT form_type,count(*) FROM landing GROUP BY form_type;'''):
				values = self.db.Values('''SELECT trip_length FROM trip_details,landing WHERE trip_details.trip=landing.trip AND form_type=?;''',[form_type])
				if len(values)>1:
					values.sort()
					p95 = round(values[int(len(values)*0.95)-1],2)
					cutoffs[form_type] = p95
					self.db.Execute('''INSERT INTO trip_lengths VALUES(?,?,?);''',(form_type,count,p95))
			##.2
			self.db.Alter('''ALTER TABLE trip_details ADD COLUMN best_date DATE;''')
			self.db.Execute('''UPDATE trip_details SET best_date=landing_datetime;''')
			##.3
			self.db.Execute('''UPDATE trip_details SET best_date=end_datetime WHERE landing_datetime<start_datetime;''')
			##.4
			for form_type,cutoff in cutoffs.items():
				self.db.Execute('''UPDATE trip_details SET best_date=(SELECT end_datetime FROM trip_details LEFT JOIN landing ON trip_details.trip=landing.trip WHERE form_type=? AND trip_length>? AND trip_details.landing_datetime<end_datetime AND trip_length_alt<=?);''',(form_type,cutoff,cutoff))
			##.5
			self.db.Execute('''UPDATE trip_details SET best_date=? WHERE landing_datetime>?;''',(lastValidDate,lastValidDate))
			##'Round' off to a date
			self.db.Execute('''UPDATE trip_details SET best_date=strftime('%Y-%m-%d',best_date);''')
			
			self.db.Alter('''ALTER TABLE trip_details ADD COLUMN fishing_year INTEGER;''') 
			self.db.Execute('''UPDATE trip_details SET fishing_year=strftime('%Y',best_date);''')
			self.db.Execute('''UPDATE trip_details SET fishing_year=fishing_year+1 WHERE strftime('%m',best_date)>="10";''')
			self.db.Execute('''CREATE INDEX IF NOT EXISTS trip_details_fishing_year ON trip_details(fishing_year);''')

		if self.groom_estimated_subcatch:
			## Done before landings to correct any estimated catches before comparing to landings
			Init('estimated_subcatch')
			##CTN: check estimated catch if groom_estimated_subcatch_catch is specified
			##which should only be for species that use count for catch_weight (tuna species and others?)
			self.db.Execute('''DROP TABLE IF EXISTS estimated_subcatch_CTN''')
			self.db.Execute('''CREATE TABLE estimated_subcatch_CTN (species TEXT,records INTEGER);''')
			if self.groom_estimated_subcatch_catch is not None:
				##For each species in the estimated_subcatch table
				for species,count in self.db.Rows('''SELECT species_code,count(*) FROM estimated_subcatch  WHERE species_code IS NOT NULL GROUP BY species_code ORDER BY count(*)  DESC;'''): 
					self.db.Execute('''INSERT INTO estimated_subcatch_CTN VALUES(?,?);''',(species,count))
					##Compare each trips estimated catch to landed green weight to see if it lies below minimum.
					##If it does then it is assumed to represent a weight and so is converted using the average value
					minimum,average = self.groom_estimated_subcatch_catch[species]			
					##I found it necessary to create temporary tables with indices on them to get adequate execution times.  Using subqueries was much slower!
					self.db.Script('''
						CREATE TEMPORARY TABLE estimated_subcatch_CTN_%(species)s_g AS SELECT trip,sum(green_weight) AS sum_green_weight FROM landing WHERE species_code='%(species)s' AND trip IS NOT NULL GROUP BY trip;
						CREATE INDEX estimated_subcatch_CTN_%(species)s_g_trip ON estimated_subcatch_CTN_%(species)s_g(trip);
						
						CREATE TEMPORARY TABLE estimated_subcatch_CTN_%(species)s_c AS SELECT trip,sum(estimated_subcatch.catch_weight) AS sum_catch_weight FROM fishing_event,estimated_subcatch WHERE fishing_event.event_key=estimated_subcatch.event_key AND species_code='%(species)s' AND trip IS NOT NULL GROUP BY trip;
						CREATE INDEX estimated_subcatch_CTN_%(species)s_c_trip ON estimated_subcatch_CTN_%(species)s_c(trip);
						
						DROP TABLE IF EXISTS estimated_subcatch_CTN_%(species)s;
						CREATE TABLE estimated_subcatch_CTN_%(species)s AS SELECT g.trip AS trip,sum_green_weight,sum_catch_weight FROM estimated_subcatch_CTN_%(species)s_g AS g, estimated_subcatch_CTN_%(species)s_c AS c WHERE g.trip==c.trip;
						CREATE INDEX estimated_subcatch_CTN_%(species)s_trip ON estimated_subcatch_CTN_%(species)s(trip);
						
						CREATE TEMPORARY TABLE estimated_subcatch_CTN_%(species)s_e AS SELECT event_key FROM fishing_event WHERE trip IN (SELECT trip FROM estimated_subcatch_CTN_%(species)s WHERE sum_green_weight<sum_catch_weight*%(minimum)s AND trip IS NOT NULL);
						CREATE INDEX estimated_subcatch_CTN_%(species)s_e_event_key ON estimated_subcatch_CTN_%(species)s_e(event_key);
						
						INSERT INTO estimated_subcatch_changes SELECT id,'CTN','catch_weight',catch_weight,catch_weight/%(average)s FROM estimated_subcatch WHERE species_code='%(species)s' AND event_key IN (SELECT event_key FROM estimated_subcatch_CTN_%(species)s_e);
						UPDATE estimated_subcatch SET changed='CTN', catch_weight=catch_weight/%(average)s WHERE species_code='%(species)s' AND event_key IN (SELECT event_key FROM estimated_subcatch_CTN_%(species)s_e);
						
					'''%locals())
					
			Check('estimated_subcatch','CTN','change')

		if self.groom_landing:
			Init('landing')

			##DAM,DAF
			##Sometimes there is no landing_datetime and hence no fishing_year in which case we can not scale these landings
			##Drop these trips before doing scaling because otherwise the allocation will not be correct
			Drop('landing','DAM', '''landing_datetime IS NULL''')
			Drop('landing','DAF', '''landing_datetime>'%s' '''%lastValidDate)

			##DES (Starr D.1.1): Drop invalid destination codes 
			Drop('landing','DES', '''destination_type NOT IN ('A','C','E','F','H','L','O','S','U','W')''')

			##SCR (Starr D.1.4): "Find commonly entered invalid state codes and replace with correct state code"
			Change('landing','SCR','state_code',"'GRE'",'''state_code IN ('EAT','DIS')''')
			Change('landing','SCR','state_code',"'HDS'",'''state_code='HED' ''')
			Change('landing','SCR','state_code',"'HGU'",'''state_code='TGU' ''')
			Change('landing','SCR','state_code',"'GGU'",'''state_code IN ('GGO','GGT')''')

			##STI: Remove invalid state_codes. This is a list of state_codes from 
			##http://www.fish.govt.nz/en-nz/Research+Services/Research+Database+Documentation/fish_ce/Appendix+1.htm
			##If not in this list then set to NULL
			valid = ('GRE','GUT','HGU','DRE','FIL','SKF','USK','SUR','SUR','TSK','TRF','DSC','DVC','MEA','SCT','RLT','TEN','FIN',
			'LIV','MKF','MGU','HGT','HGF','GGU','SHU','ROE','HDS','HET','FIT','SHF','MBS','MBH','MEB','FLP','BEA','LIB',
			'CHK','LUG','SWB','WIN','OIL','TNB','GBP')
			Change('landing','STI','state_code','NULL','''state_code NOT IN %s'''%repr(valid))

			##DUP (Starr D.1.2): "Look for duplicate landings on multiple (CELR and CLR) forms. Keep only a single version if determined that the records are duplicated"
			##If the following fields are duplicated across form types then drop all but the CEL record.  Do this after state_code, destination_type etc have been fixed up.
			fields = ['vessel_key','landing_datetime','fishstock_code','state_code','destination_type','unit_type','unit_num','unit_weight','green_weight']
			sql = '''UPDATE landing SET dropped='DUP' WHERE dropped IS NULL AND form_type!='CEL' '''
			for field in fields: sql += ''' AND %s=?'''%field
			fieldsComma = ','.join(fields)
			for row in self.db.Rows('''SELECT %s FROM (
					SELECT DISTINCT %s,form_type FROM landing WHERE dropped IS NULL
				) GROUP BY %s HAVING count(*)>1;'''%(fieldsComma,fieldsComma,fieldsComma)): 
				self.db.Execute(sql,row)
			Check('landing','DUP','drop',field='',to='NULL',clause=''' 'Duplicated on CELR and CLR forms' ''')
				
			##COM (Starr D.1.3): "Find missing conversion factor fields and insert correct value for relevant state code and fishing year.
			## Missing fields can be inferred from the median of the non-missing fields"
			##For each state_code replace missing values with median
			for row in self.db.Rows('''SELECT DISTINCT species_code,state_code FROM landing WHERE state_code IS NOT NULL AND conv_factor IS NOT NULL;'''):
				species_code,state_code = row
				median = self.db.Value('''SELECT median(conv_factor) FROM landing WHERE species_code=='%s' AND state_code='%s' AND conv_factor IS NOT NULL;'''%(species_code,state_code))
				if median is not None: Change('landing','COM','conv_factor',median,'''conv_factor IS NULL AND species_code=='%s' AND state_code=='%s' '''%(species_code,state_code))
					
			##COV (Starr D.1.5): See if more than one conv_factor for each state code for each species
			self.db.Execute('''CREATE TABLE IF NOT EXISTS landing_COV(
				species TEXT,
				state_code TEXT,
				conv_factor REAL,
				records INTEGER
			);''')
			for species in [row[0] for row in self.db.Rows('''SELECT DISTINCT species_code FROM landing;''')]:
				for row in self.db.Rows('''SELECT state_code,conv_factor,count(*) FROM landing WHERE state_code IN (
					SELECT state_code FROM (
						SELECT DISTINCT state_code,conv_factor FROM landing WHERE species_code=='%s' AND dropped IS NULL
					) GROUP BY state_code HAVING count(*)>1
				) GROUP BY state_code,conv_factor'''%species): self.db.Execute('''INSERT INTO landing_COV VALUES(?,?,?,?);''',(species,row[0],row[1],row[2]))

			##STD (Starr D.1.6): "Drop landings where state code==FIN|==FLP|==SHF|==ROE and there is more than one record for the trip/Fishstock combination."
			for row in self.db.Rows('''SELECT landing.trip,landing.fishstock_code FROM landing INNER JOIN (
				SELECT DISTINCT trip,fishstock_code FROM landing WHERE dropped IS NULL AND state_code IN ('FIN','FLP','SCF','ROE') AND trip IS NOT NULL
			) hasCodes ON landing.trip=hasCodes.trip AND landing.fishstock_code=hasCodes.fishstock_code
			GROUP BY landing.trip,landing.fishstock_code HAVING count(*)>1;'''
			):Drop('landing','STD','''trip=%s AND fishstock_code='%s' AND state_code IN ('FIN','FLP','SCF','ROE')'''%row)

			##GRC,GRO,GRM (Starr D.1.7): "Check for missing data in the unit_num and unit_weight fields. Drop records where greenweight=0 or =NULL and either unit_num and unit_weight is missing.
			## Missing greenweight can be estimated"
			Change('landing','GRC', 'green_weight','conv_factor*unit_num*unit_weight','''(green_weight=0 OR green_weight IS NULL) AND conv_factor IS NOT NULL''')
			Change('landing','GRO','green_weight','unit_num*unit_weight','''(green_weight=0 OR green_weight IS NULL) AND conv_factor IS NULL''')
			Drop('landing','GRM','''(green_weight=0 OR green_weight IS NULL) AND (unit_num IS NULL OR unit_weight IS NULL)''')

			##GRR (Starr D.1.9): "Check for out of range landings"
			self.db.Execute('''DROP TABLE IF EXISTS landing_GRR''')
			self.db.Execute('''CREATE TABLE IF NOT EXISTS landing_GRR (species TEXT,method TEXT,events INTEGER,proportion REAL,landings_threshold REAL,cpue_threshold REAL);''')
			if 'GRR' not in self.groom_excludes:
				
				class Trip(object):
					
					def __init__(self):
						self.sum_green_weight = None
						self.sum_calc_weight = None
						self.sum_est_weight = None
						self.ratio_green_calc = None
						self.ratio_green_est = None
						self.sum_effort = None
						self.cpue = None
						self.green_high = 0
						self.ok = 0
						self.drop = 0

				##.1 "Find all landing events which are greater than the appropriate ProcA value. Values smaller than ProcA
				## can be used to make a more complete search of the data. Identify the trip numbers associated with these
				## landing events. Calculate for these trips: a) the total greenweight; b) the calculated greenweight"
				for species,count in self.db.Rows('''SELECT species_code,count(*) FROM landing  WHERE dropped IS NULL AND species_code IS NOT NULL GROUP BY species_code HAVING count(*)>=%s ORDER BY count(*)  DESC;'''%self.groom_GRR_species_min): 
					
					print species,count
					
					trips = {}
					##.2 "Extract the fishing event data for these trips. Summarise for the trips using method m: a) the total effort; b) the total estimated catch. 
					## Calculate the nominal CPUE (Eq. 1) for each trip t with large landings using method m"
					
					##Need to caculate things for ALL trips because these are used to determine distributions of CPUE etc
						##Paul is using two ratios to further narrow down from this low threshold:
						##	1: sum_green_weight/sum_calc_weight
						##	2: sum_green_weight/sum_catch_weight
						##So need to calculate some of these from landings data
					
					for row in self.db.Rows('''SELECT trip,sum(green_weight),sum(conv_factor*unit_num*unit_weight) FROM landing WHERE trip IS NOT NULL AND species_code='%s' GROUP BY trip;'''%species): 
						trip = Trip()
						trip.sum_green_weight = row[1]
						trip.sum_calc_weight = row[2]
						if trip.sum_green_weight is not None and trip.sum_calc_weight>0: trip.ratio_green_calc = trip.sum_green_weight/trip.sum_calc_weight
						trips[row[0]] = trip
						
					##Caclulate sum of estimated catch for trip
					##If this is a 'numbers' species then multiply by the average weight
					if self.groom_estimated_subcatch_catch is not None: 
						minimum,average = self.groom_estimated_subcatch_catch[species]	
						adjust = '*%s'%average
					else: adjust = ''
					sql = '''SELECT trip,sum(estimated_subcatch.catch_weight)%s FROM fishing_event,estimated_subcatch WHERE fishing_event.event_key=estimated_subcatch.event_key AND trip IS NOT NULL AND species_code='%s' GROUP BY trip;'''%(adjust,species)
					for row in self.db.Rows(sql):
						try: trip = trips[row[0]]
						except KeyError: continue ##There may be no match because the trip did not land the species but did record it in effort. That does not matter here because were are concerned with landings
						trip.sum_est_weight = row[1]
						if trip.sum_green_weight is not None and trip.sum_est_weight>0: trip.ratio_green_est = trip.sum_green_weight/trip.sum_est_weight
					
					##Determine the most important methods for this species in the dataset by finding those that account for 80% of the catch
					overall = self.db.Value('''SELECT count(*) FROM fishing_event WHERE trip IN (SELECT DISTINCT trip FROM landing WHERE species_code=='%s' AND trip IS NOT NULL);'''%species) 
					cumulative = 0.0
					for method,count in self.db.Rows('''SELECT primary_method,count(*) FROM fishing_event WHERE trip IN (SELECT DISTINCT trip FROM landing WHERE species_code=='%s'  AND trip IS NOT NULL) GROUP BY primary_method HAVING count(*)>=100 ORDER BY count(*) DESC;'''%species):
						if cumulative>0.8: break
						proportion = float(count)/overall
						cumulative += proportion
							
						print method,count,proportion,cumulative
						
						##Clear method related data from trips so it does not 'hangover' from one method to the next
						for trip in trips.values():
							trip.sum_effort = None
							trip.cpue = None
							trip.green_high = 0
							trip.drop = 0
						
						effort_units = {
							'BLL':'total_hook_num',
							'SN':'net_length'
						}.get(method,'effort_num')
						
						##Calculate CPUE distribution for this method for each trip
						cpues = []
						for row in self.db.Rows('''SELECT trip,sum(%s) FROM fishing_event WHERE primary_method='%s' AND trip IN (SELECT DISTINCT trip FROM landing WHERE species_code=='%s' AND trip IS NOT NULL) GROUP BY trip;'''%(effort_units,method,species)): 
							trip = trips[row[0]]
							trip.sum_effort = row[1]
							if trip.sum_green_weight>0 and trip.sum_effort>0:
								trip.cpue = trip.sum_green_weight/trip.sum_effort
								if trip.ratio_green_est>0.75 and  trip.ratio_green_est<1.33: 
									trip.ok = 1
									cpues.append(trip.cpue)
						
						##Calculate thresholds for CPUE: 2*95th percentile
						cpues.sort()
						index = int(len(cpues)*0.95)-1
						if index<len(cpues) and index>=0: cpue_threshold = 2 * cpues[index]
						elif len(cpues)>=1: cpue_threshold = 2 * cpues[0]
						else: cpue_threshold = 1e10 ##i.e. No CPUE threshold

						##Find all trips that have at least one landing greater than the threshold. 
						landings_threshold = self.db.Value('''SELECT proc_a FROM dqss WHERE species=? AND method=?''',(species,method))
						if landings_threshold is None: landings_threshold = 1.0
							
						for row in self.db.Rows('''SELECT DISTINCT trip FROM fishing_event WHERE primary_method=? AND trip IN (
							SELECT DISTINCT trip FROM landing WHERE trip IS NOT NULL AND dropped IS NULL AND species_code==? AND green_weight>?)''',[method,species,landings_threshold]):
							trip = trips[row[0]]
							trip.green_high = 1
							if (trip.ratio_green_calc>4 or trip.ratio_green_est>4) and trip.cpue>cpue_threshold: trip.drop = 1		
						
						for id,trip in trips.items():
							if trip.drop==1: self.db.Execute('''UPDATE landing SET dropped='GRR %s %s' WHERE dropped IS NULL AND trip=?;'''%(species,method),[id])
								
						Check('landing','GRR','drop',field='',to='NULL',clause=''' %s %s %s %s %s  '''%(species,method,round(proportion*100,1),round(landings_threshold,2),round(cpue_threshold,2)))
						
						##Store results for each species/method
						self.db.Execute('''INSERT INTO landing_GRR(species,method,events,proportion,landings_threshold,cpue_threshold) VALUES(?,?,?,?,?,?);''',(species,method,count,proportion,landings_threshold,cpue_threshold))
						
						self.db.Execute('''DROP TABLE IF EXISTS landing_GRR_%s_%s'''%(species,method))
						self.db.Execute('''CREATE TABLE IF NOT EXISTS landing_GRR_%s_%s (trip INTEGER,sum_green_weight REAL, sum_calc_weight REAL, sum_est_weight REAL, ratio_green_calc REAL, ratio_green_est REAL, sum_effort REAL, cpue REAL, ok INTEGER, green_high INTEGER, dropped INTEGER);'''%(species,method))
						self.db.Cursor.executemany('''INSERT INTO landing_GRR_%s_%s VALUES(?,?,?,?,?,?,?,?,?,?,?)'''%(species,method),[(id, trip.sum_green_weight, trip.sum_calc_weight, trip.sum_est_weight, trip.ratio_green_calc, trip.ratio_green_est, trip.sum_effort, trip.cpue, trip.ok, trip.green_high, trip.drop) for id,trip in trips.items()])

		if self.groom_fishing_event:	
				
			Init('fishing_event')
			
			Drop('fishing_event','DAM', '''start_datetime IS NULL''')
			Drop('fishing_event','DAF', '''start_datetime>'%s' '''%lastValidDate)
			
			##Set lat and lon to NULL where 999.9
			self.db.Execute('''UPDATE fishing_event SET effort_num=1 WHERE form_type='TCP';''')
			##Set lat and lon to NULL where 999.9
			for field in ('trunc_start_lat','trunc_start_long','trunc_end_lat','trunc_end_long'): self.db.Execute('''UPDATE fishing_event SET %s==NULL WHERE %s=999.9'''%(field,field))
			
			##PMR/M (Starr D.2.1) "Look for missing method codes by trip. a) drop the entire trip if more than one method was used for the trip; b) if a single 
			##method trip, insert the method into the missing field."
			##Find all records with missing primary_method. For each trip in this set count number of primary_method used. If only one, then replace
			##otherwise drop the trip
			for row in self.db.Rows('''SELECT trip,primary_method FROM (
				SELECT DISTINCT trip,primary_method FROM fishing_event WHERE trip IN (SELECT DISTINCT trip FROM fishing_event WHERE primary_method IS NULL AND trip IS NOT NULL) AND primary_method IS NOT NULL
			) GROUP BY trip HAVING count(*)==1'''): self.db.Execute('''UPDATE fishing_event SET primary_method=?,changed='PMR' WHERE primary_method IS NULL AND trip=?''',[row[1],row[0]])
			##Now if still has a NULL primary_method, drop the entire trip
			Drop('fishing_event','PMM',''' trip IN (SELECT DISTINCT trip FROM fishing_event WHERE primary_method IS NULL AND trip IS NOT NULL)''')

			##SAR/M (Starr D.2.2): "Search for missing statistical area fields. a) drop the entire trip if all statistical areas are missing in the trip;
			##b) substitute the 'predominant' (most frequent) statistical area for the trip for trips which report the statistical area fished in other records."
			stats_valid = '''('001','002','003','004','005','006','007','008','009','009H','010','011','012','013','014','015','016','017','018','019','020','021','022','023','024','025','026','027','028','029','030','031','032','033','034','035','036','037','038','039',
				'040','041','042','043','044','045','046','047','048','049','050','051','052','091','092','093','094','101','102','103','104','105','106','107','201','202','203','204','205','206','301','302','303',
				'401','402','403','404','405','406','407','408','409','410','411','412','501','502','503','504','601','602','603','604','605','606','607','608','609','610','611','612','613','614','615','616','617','618','619','620','621','622','623','624','625',
				'701','702','703','704','705','706','801')'''
			##There are more valid stat areas than the ones above  (e.g. rock lobster and paua stat areas) so this is not a complete list and can't be used as it is
			for trip in self.db.Values('''SELECT DISTINCT trip FROM fishing_event WHERE start_stats_area_code IS NULL AND trip IS NOT NULL'''):
				stat = self.db.Value('''SELECT start_stats_area_code FROM fishing_event WHERE start_stats_area_code IS NOT NULL AND trip=? GROUP BY start_stats_area_code ORDER BY count(*) DESC LIMIT 1''',[trip])
				if stat: 
					self.db.Execute('''INSERT INTO fishing_event_changes SELECT id,'SAR','start_stats_area_code',start_stats_area_code,'%s' FROM fishing_event WHERE start_stats_area_code IS NULL AND trip=?'''%stat,[trip])
					self.db.Execute('''UPDATE fishing_event SET start_stats_area_code='%s',changed='SAR' WHERE start_stats_area_code IS NULL AND trip=?'''%stat,[trip])
			Drop('fishing_event','SAM','''trip IN (SELECT DISTINCT trip FROM fishing_event WHERE start_stats_area_code IS NULL AND trip IS NOT NULL)''')
			
			##TAR/M (Starr D.2.3). "Search for missing target species fields. a) drop the entire trip if all target species are missing in the trip; 
			## b) substitute the 'predominant' (most frequent) target species for the trip for trips which report the target species in other records"
			for trip in self.db.Values('''SELECT DISTINCT trip FROM fishing_event WHERE target_species IS NULL AND trip IS NOT NULL'''):
				target_species = self.db.Value('''SELECT target_species FROM fishing_event 
					WHERE trip=? AND target_species IS NOT NULL GROUP BY target_species ORDER BY count(*) DESC LIMIT 1''',(trip,))
				if target_species: self.db.Execute('''UPDATE fishing_event SET target_species=?,changed='TAR' WHERE target_species IS NULL AND trip=?''',[target_species,trip])
			Drop('fishing_event','TAM','''trip IN (SELECT DISTINCT trip FROM fishing_event WHERE target_species IS NULL)''')
			
			##POS: Drop positions that are outside of the bounding box of the statistical areas
			self.db.Execute('''DROP TABLE IF EXISTS stats_boxes;''')  
			self.db.Execute('''CREATE TABLE stats_boxes(stat TEXT,latmin REAL,latmax REAL,lonmin REAL,lonmax REAL);''')  
			stat_boxes = file("/Trophia/Tanga/Data/shared/stats_boxes.txt")
			for values in [line.split(' ') for line in stat_boxes.read().split('\n') if len(line)>0]: self.db.Execute('''INSERT INTO stats_boxes VALUES(?,?,?,?,?);''',values)
			for stat,latmin,latmax,lonmin,lonmax in self.db.Rows('''SELECT * FROM stats_boxes;'''):
				self.db.Execute('''UPDATE fishing_event SET trunc_start_lat=NULL, changed='POS lat' WHERE start_stats_area_code=='%s' AND trunc_start_lat NOT BETWEEN %s-0.1 AND %s+0.1;'''%(stat,latmin,latmax))
				self.db.Execute('''UPDATE fishing_event SET trunc_start_long=NULL, changed='POS long' WHERE start_stats_area_code=='%s' AND trunc_start_long NOT BETWEEN %s-0.1 AND %s+0.1;'''%(stat,lonmin,lonmax))
			
			## EFF(Starr D.2.4): "Operate grooming procedure on effort fields by method of capture and form type to truncate outlier values."
			self.db.Script('''
			DROP TABLE IF EXISTS fishing_event_EFF;
			CREATE TABLE fishing_event_EFF (
				field TEXT,
				form_type TEXT,
				primary_method TEXT,
				count INTEGER,
				percent REAL,
				min REAL,
				p10 REAL,
				median REAL ,
				p90 REAL,
				max REAL,
				floor REAL,
				multiplier REAL,
				lower REAL,
				upper REAL,
				substitutions REAL);''')
				
			##Only do this for methods we are interested in (can be slow and look silly in grooming reports otherwise)
			if len(self.methods)==0: method_clause = 'primary_method IS NOT NULL'
			else: method_clause = 'primary_method IN (%s)'%(','.join([repr(item) for item in self.methods]))
			
			for field in [
				'effort_num',
				'fishing_duration',
				'total_hook_num',
				'total_net_length',
			]:
				##"1. For a given effort field, select a partial dataset from the fishing event data based on a method of capture and a form type (CELR or TCEPR)"
				##For each form_type & primary method combination which has at least 1000 not NULL records and >=80% of records not NULL
				for form_type,primary_method,count in self.db.Rows('''
					SELECT form_type,primary_method,count(*) 
					FROM fishing_event 
					WHERE form_type IS NOT NULL AND %s AND %s IS NOT NULL 
					GROUP BY form_type,primary_method
					HAVING count(*)>=1000
					ORDER BY count(*) DESC
					LIMIT 10'''%(method_clause,field)):
					print field,form_type,primary_method,count,; sys.stdout.flush()
					
					percent = float(count)/self.db.Value('''SELECT count(*) FROM fishing_event WHERE primary_method=? AND form_type=?''',(primary_method,form_type))*100
					if percent<80: continue
					
					##"2. Calculate the 10th, 50th and 90th percentiles for this effort field within this dataset. Multiply the 10th and 90th
					##percentiles by trial multipliers (often 1/2X and 2X; Table 5) respectively to get the lower and upper bounds
					##which define the outliers."
					values = self.db.Values('''SELECT %s FROM fishing_event WHERE primary_method=? AND form_type=? AND %s IS NOT NULL'''%(field,field),(primary_method,form_type))
					values.sort()
					n = len(values)
					mi,p10,fleet_median,p90,ma = values[0],values[int(n*0.1)-1],values[int(n*0.5)-1],values[int(n*0.9)-1],values[-1]
					
					##"3. If a 'floor' value is required, then substitute the 'floor' value for the calculated lower bound (e.g., there is no
					## point in having a lower bound that is smaller than 1 tow for a trawl method. See Table 5 for floor values used
					## in some analyses)."
					floor = 0
					if field=='effort_num': floor = 1
					elif field=='fishing_duration':
						if primary_method=='MW': floor = 5/60.0 ##5 minutes
						elif primary_method=='BT':floor = 10/60.0 ## 10 minutes
						elif primary_method=='SN':floor = 2 ## 2 hours
						else: floor = 5/60.0 ##5 minutes
					print p10,fleet_median,p90,floor,; sys.stdout.flush()
					
					multiplier = 2.0
					substitution_thresh = 0.01
					while True:
						print '.',; sys.stdout.flush()
						
						lower,upper = p10/multiplier,p90*multiplier
						if lower<floor: lower = floor
						
						##"6. Compare the effort value in every record with the trial bounds determined in Step 1. Substitute the vessel
						##median if the effort value lies outside of the bounds."
						##At this stage I just count the number of substitutions that would be made given lower and upper
						##Actual substitutions are not done until the right multiplier has been determined based on substiution proportion
						##This avoids calculating vessel median for every trial value of multiplier
						substitutions = self.db.Value('''SELECT count(*) FROM fishing_event WHERE primary_method=? AND form_type=? AND (%s IS NULL OR (%s<? OR %s>?))'''%(field,field,field),(primary_method,form_type,lower,upper))/float(n)
						
						##"7. Repeat this procedure with alternative bounds if the number of substitutions is larger than 1% (changed from 5% to be more conservative in changing data) of the total
						##number of records."
						if substitutions<=substitution_thresh:break
						else:
							if multiplier>=10: break ##If multiplier needs to be higher than this then something wrong that probaly can't be dealt with by this method e.g. bimodality
							multiplier += 0.1
					print multiplier,substitutions,; sys.stdout.flush()
					
					if substitutions<=substitution_thresh:
						##"4. Calculate the median value of the effort field for every vessel in the selected dataset. Substitute the median
						##value calculated in Step 1 if the median value for the vessel lies outside of the calculated bounds."
						##Find vessels that have any values for field outside of bounds
						##"5. Create a temporary field on every record in the partial dataset which contains the median value of the target
						##effort field for the vessel appropriate to the record. This field will contain the fleet median for vessels which
						##were outside of the bounds as defined in Step 3."
						for vessel in self.db.Values('''SELECT DISTINCT vessel_key FROM fishing_event WHERE vessel_key IS NOT NULL AND primary_method=? AND form_type=? AND (%s IS NULL OR (%s<? OR %s>?))'''%(field,field,field),(primary_method,form_type,lower,upper)):
							##Calculate median
							vessel_median = self.db.Value('''SELECT median(%s) FROM fishing_event WHERE vessel_key=? AND primary_method=? AND form_type=? AND %s IS NOT NULL'''%(field,field),(vessel,primary_method,form_type))
							##Replace vessel_median with fleet_median if outside of bounds or never recorded any effort of this method
							if vessel_median is None or vessel_median<lower or vessel_median>upper: vessel_median = fleet_median
							##Record replacement
							self.db.Execute('''INSERT INTO fishing_event_changes SELECT id,'EFF %s','%s',%s,? FROM fishing_event WHERE vessel_key=? AND primary_method=? AND form_type=? AND (%s IS NULL OR (%s<? OR %s>?)); '''%(field,field,field,field,field,field),(vessel_median,vessel,primary_method,form_type,lower,upper))
							##Do replacement 
							self.db.Execute('''UPDATE fishing_event SET %s=?, changed='EFF %s' WHERE vessel_key=? AND primary_method=? AND form_type=? AND (%s IS NULL OR (%s<? OR %s>?)) '''%(field,field,field,field,field),(vessel_median,vessel,primary_method,form_type,lower,upper))
					else:
						##Take a different approach
						multiplier = 2.0
						lower,upper = p10/multiplier,p90*multiplier
						if lower<floor: lower = floor
						self.db.Execute('''UPDATE fishing_event SET %s=?,changed='EFL %s' WHERE primary_method=? AND form_type=? AND %s<? '''%(field,field,field),(lower,primary_method,form_type,lower))
						self.db.Execute('''UPDATE fishing_event SET %s=?,changed='EFU %s' WHERE primary_method=? AND form_type=? AND %s>? '''%(field,field,field),(upper,primary_method,form_type,upper))
						self.db.Execute('''UPDATE fishing_event SET %s=?,changed='EFM %s' WHERE primary_method=? AND form_type=? AND %s IS NULL '''%(field,field,field),(fleet_median,primary_method,form_type))
						
					print '-',; sys.stdout.flush()
					
					##Record
					self.db.Execute('''INSERT INTO fishing_event_EFF VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);''',[field,form_type,primary_method,count,percent,mi,p10,fleet_median,p90,ma,round(floor,3),multiplier,lower,upper,round(substitutions*100,2)])
					
					print
			
			##Starr paragraph D.2.3 says that for CPUE studies  trips which have data changed under D.2.4 (paragraph D.2.2) should be dropped
			##unless the form type is TCPER. I create a new field which acts as a field for whether the record should be used in CPUE analyses
			self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN cpue_no INTEGER;''')
			for row in self.db.Rows('''SELECT DISTINCT trip FROM fishing_event WHERE changed LIKE "EF%" AND form_type!='TCP' AND trip IS NOT NULL;'''): self.db.Execute('''UPDATE fishing_event SET cpue_no=1 WHERE trip=?''',row)
			
		self.db.Execute('''INSERT INTO status(task,done) VALUES('groom',datetime('now'));''')
		self.db.Commit()

	def allocate(self):
		'''Allocate landings to fishing_events for the top 10 species in landings'''
		
		self.db.Execute('''DROP TABLE IF EXISTS allocations''')
		self.db.Execute('''CREATE TABLE allocations (species_code TEXT, fishstock_code TEXT, trips_tried INTEGER,trips_failed INTEGER);''')

		for species in self.species:
			print species
			
			self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN %s_est REAL;'''%species)  
			self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN %s_equ REAL;'''%species) 
			self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN %s_prop REAL;'''%species) 
			self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN %s_prop_method INTEGER;'''%species)
			self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN %s_fishstocks INTEGER;'''%species)
			
			self.db.Execute('''UPDATE fishing_event SET %s_est=0,%s_equ=0,%s_prop=0,%s_fishstocks=0;'''%(species,species,species,species)) 
			
			##Update estimated catch (I tried several ways and although it looks inelegant, it is by far the fastest.  Change at your own risk!)
			for event_key,catch_weight in self.db.Rows('''SELECT event_key,sum(catch_weight) FROM estimated_subcatch WHERE species_code='%s' GROUP BY event_key;'''%species):
				if catch_weight is None: catch_weight = 0
				self.db.Execute('''UPDATE fishing_event SET %s_est=%s WHERE event_key=%s;'''%(species,catch_weight,event_key))
			
			##Starr E.1.5 & E.1.6 & F Allocate landings to effort_collapsed strata
			##Allocate each trip/fishstock stratum in landings_collapsed to matching trip/stat/method/target stratums in effort collapsed
			##Allocation is done on two bases (a) equally divided (b) relative to estimated catch (or on the basis of effort_num if there was no estimated catch,
			## or equally if a mixed method trip or no effort_num
			##Decide which fishtock landings to allocate.  Normally will be just one
			if self.allocate_fishstocks is None: fishstocks =  self.fishstocks[species]
			elif self.allocate_fishstocks=='all': fishstocks = self.db.Values('''SELECT fishstock_code FROM landing WHERE species_code='%s' AND fishstock_code NOT NULL GROUP BY fishstock_code ORDER BY count(*) DESC;'''%species)
			else: fishstocks = self.allocate_fishstocks
			fishstocks_list = ','.join([repr(str(fishstock)) for fishstock in fishstocks])
				
			##OTH: Drop fishing events that where in statistical areas other than this fishstock
			##We don't want to allocate anything to them
			self.db.Execute('''UPDATE fishing_event SET dropped='OTH' WHERE start_stats_area_code NOT IN (SELECT stat FROM qmastats WHERE qma IN (%s));'''%(fishstocks_list))

			##STR Starr E.1.4 Mark trips which landed to more than one **other** (not in fishstocks) fishstock for straddling statistical areas
			##Since this relies on fishing event.start_stats_area_code do this after grooming on that table
			self.db.Script('''
			DROP TABLE IF EXISTS fishing_event_STR;
			CREATE TABLE IF NOT EXISTS fishing_event_STR(
				species TEXT,
				stat TEXT,
				trip INTEGER,
				other TEXT
			);''')
			for stat in self.db.Values('''SELECT DISTINCT start_stats_area_code FROM fishing_event WHERE
				dropped IS NULL AND
				start_stats_area_code IN (SELECT stat FROM qmastats WHERE species=='%s' GROUP BY stat HAVING count(*)>=2);'''%species):
				##Get the fishstocks that could be in this stat
				fishstocks_stat = ','.join([repr(str(value)) for value in self.db.Values('''SELECT qma FROM qmastats WHERE species='%s' AND stat='%s';'''%(species,stat))])
				print stat, fishstocks_stat,fishstocks_list
				##For each trip that fished in this stat...
				for index,trip in enumerate(self.db.Values('''SELECT DISTINCT trip FROM fishing_event WHERE start_stats_area_code='%s' AND trip IS NOT NULL'''%stat)):
					##...see if there was any fishing in any other fishstock
					other = self.db.Value('''SELECT fishstock_code FROM landing WHERE trip=%s AND fishstock_code IN (%s) AND fishstock_code NOT IN (%s) LIMIT 1;'''%(trip,fishstocks_stat,fishstocks_list))
					self.db.Execute('''INSERT INTO fishing_event_STR(species,stat,trip,other) VALUES(?,?,?,?);''',(species,stat,trip,other))
					if other is not None and self.allocate_straddles_drop:  
						self.db.Execute('''UPDATE fishing_event SET dropped='STR %s %s' WHERE trip=%s'''%(species,stat,trip))
						self.db.Execute('''UPDATE landing SET dropped='STR %s %s' WHERE trip=%s'''%(species,stat,trip)) ##This is required for reporting

			##Allocate landings for each fishstock...
			for fishstock in fishstocks:
				stats = ','.join([repr(str(value)) for value in self.db.Values('''SELECT stat FROM qmastats WHERE qma='%s' '''%fishstock)])
				print fishstock,stats
				trips_tried,trips_failed = 0,0
				##...for each trip...
				for trip,sum_green_weight in self.db.Rows('''SELECT trip,sum(green_weight) FROM landing WHERE fishstock_code='%s' AND dropped IS NULL AND trip IS NOT NULL GROUP BY trip;'''%fishstock):
					trips_tried+= 1
					if trips_tried%1000==0: 
						sys.stdout.write('.')
						sys.stdout.flush()
					count,sum_effort_num,sum_catch_weight = self.db.Row('''SELECT count(*),sum(effort_num),sum(%s_est) FROM fishing_event WHERE dropped IS NULL AND trip=%s AND start_stats_area_code IN (%s)'''%(species,trip,stats))
					if sum_effort_num is None: sum_effort_num = 0
					if sum_catch_weight is None: sum_catch_weight = 0
					if count==0: trips_failed += 1
					else:
						equ = sum_green_weight/float(count)
						self.db.Execute('''UPDATE fishing_event SET %s_equ=%s_equ+%s WHERE dropped IS NULL AND trip=%s AND start_stats_area_code IN (%s)'''%(species,species,equ,trip,stats))
						if sum_catch_weight>0:
							for event_key,catch_weight in self.db.Rows('''SELECT event_key,%s_est FROM fishing_event WHERE dropped IS NULL AND trip=%s AND start_stats_area_code IN (%s);'''%(species,trip,stats)):
								if catch_weight is None: catch_weight = 0
								prop = sum_green_weight * catch_weight/float(sum_catch_weight)
								self.db.Execute('''UPDATE fishing_event SET %s_prop=%s_prop+%s,%s_fishstocks=%s_fishstocks+1,%s_prop_method=1 WHERE event_key=%s;'''%(species,species,prop,species,species,species,event_key))
						else:
							methods = self.db.Values('''SELECT DISTINCT primary_method FROM fishing_event WHERE dropped IS NULL AND trip=%s AND start_stats_area_code IN (%s)'''%(trip,stats))
							if len(methods)==1 and sum_effort_num>0:
								for event_key,effort_num in self.db.Rows('''SELECT event_key,effort_num FROM fishing_event WHERE dropped IS NULL AND trip=%s AND start_stats_area_code IN (%s)'''%(trip,stats)):
									if effort_num is None: effort_num = 0
									prop = sum_green_weight * effort_num/float(sum_effort_num)
									self.db.Execute('''UPDATE fishing_event SET %s_prop=%s_prop+%s,%s_fishstocks=%s_fishstocks+1,%s_prop_method=2 WHERE event_key=%s'''%(species,species,prop,species,species,species,event_key))
							else: self.db.Execute('''UPDATE fishing_event SET %s_prop=%s_prop+%s,%s_fishstocks=%s_fishstocks+1,%s_prop_method=3 WHERE dropped IS NULL AND trip=%s AND start_stats_area_code IN (%s)'''%(species,species,equ,species,species,species,trip,stats))
				print
				
				self.db.Execute('''INSERT INTO allocations VALUES(?,?,?,?);''',(species,fishstock,trips_tried,trips_failed))

		self.db.Execute('''INSERT INTO status(task,done) VALUES('allocate',datetime('now'));''')
		self.db.Commit()

	def augment(self):

		##Create catch totals (if more than one species)
		if len(self.species)>1:
			for field in ('est','equ','prop'):
				self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN TOT_%s REAL;'''%field)  
				self.db.Execute('''UPDATE fishing_event SET TOT_%s=%s;'''%(field,'+'.join([specie+'_'+field for specie in self.species])))
					
		##Calculate date and fishing_year (used for reporting on effect of grooming)
		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN date DATETIME;''')  
		self.db.Execute('''UPDATE fishing_event SET date=strftime('%Y-%m-%d',start_datetime);''')
		self.db.Execute('''CREATE INDEX IF NOT EXISTS fishing_event_date ON fishing_event(date);''')

		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN fishing_year INTEGER;''') 
		self.db.Execute('''UPDATE fishing_event SET fishing_year=strftime('%Y',date);''')
		self.db.Execute('''UPDATE fishing_event SET fishing_year=fishing_year+1 WHERE strftime('%m',date)>="10";''')
		self.db.Execute('''CREATE INDEX IF NOT EXISTS fishing_event_fishing_year ON fishing_event(fishing_year);''')

		self.db.Alter('''ALTER TABLE landing ADD COLUMN fishing_year INTEGER;''') 
		self.db.Execute('''UPDATE landing SET fishing_year=strftime('%Y',landing_datetime);''')
		self.db.Execute('''UPDATE landing SET fishing_year=fishing_year+1 WHERE strftime('%m',landing_datetime)>="10";''')
		self.db.Execute('''CREATE INDEX IF NOT EXISTS landing_fishing_year ON landing(fishing_year);''')

		##Add fishing_year to fishing_event, qmr and mhr table for summing
		for table in ('qmr','mhr'):
			self.db.Alter('''ALTER TABLE %s ADD COLUMN fishing_year INTEGER;'''%table)  
			self.db.Execute('''UPDATE %s SET fishing_year=year WHERE month<=9;'''%table)  
			self.db.Execute('''UPDATE %s SET fishing_year=year+1 WHERE month>=10;'''%table)  
		self.db.Execute('''CREATE INDEX IF NOT EXISTS qmr_fishstock_fishing_year ON qmr(fishstock,fishing_year);''')  
		self.db.Execute('''CREATE INDEX IF NOT EXISTS mhr_stock_code_fishing_year ON mhr(stock_code,fishing_year);''')  

		##Add a 'vessel day' which is simply one over the number of records in a day.  This is useful because when summed together it gives the number
		##of unique days fishedd by the vessel.  For CELR this is not equivalent to a record since the there is a new record if they change stat area or primary_method.
		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN vessel_day REAL;''') 
		self.db.Execute('''CREATE INDEX IF NOT EXISTS fishing_event_vessel_key_date ON fishing_event(vessel_key,date);''')
		self.db.Execute('''UPDATE fishing_event SET vessel_day=(SELECT 1.0/count(*) FROM fishing_event AS fe WHERE fe.vessel_key=fishing_event.vessel_key AND fe.date=fishing_event.date);''')

		##Add inshore
		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN inshore BOOLEAN;''')  
		self.db.Execute('''UPDATE fishing_event SET inshore=(start_stats_area_code<="052");''')  

		##Add zones (statistical area groups based on inshore areas)
		##Zones are different for each Fishstock so need to decide which to use - use the first fishstock in the allocations table since this is for the first fishstock for the first species
		fishstock = self.db.Value('''SELECT fishstock_code FROM allocations LIMIT 1;''')

		self.db.Execute('''DROP TABLE IF EXISTS stats_zones;''')  
		self.db.Execute('''CREATE TABLE stats_zones(stat TEXT,zone TEXT);''')  
		if self.zones is None:
			zones = file("/Trophia/Tanga/Data/shared/stats_zones.txt")
			zones.readline() ##Header
			for values in [line.split('\t') for line in zones.read().split('\n') if len(line)>0]:  
				if values[1]==fishstock:  self.db.Execute('''INSERT INTO stats_zones VALUES(?,?);''',(values[2],values[3]))
		else:
			for zone,stats in self.zones.items(): 
				for stat in stats: self.db.Execute('''INSERT INTO stats_zones VALUES(?,?);''',(stat,zone))
				
		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN zone TEXT;''')  
		self.db.Execute('''UPDATE fishing_event SET zone=(SELECT zone FROM stats_zones WHERE stats_zones.stat=fishing_event.start_stats_area_code);''')  
		self.db.Execute('''UPDATE fishing_event SET zone='Other' WHERE zone IS NULL;''')  

		self.db.Execute('''INSERT INTO status(task,done) VALUES('augment',datetime('now'));''')
		self.db.Commit()

	def simplify(self):
		'''Create a characterization database that is optimized to provide summaries.
		Create a separate database to reduce size when transferring to server and for security reasons'''

		##Random number for manging vessel_keys
		vessel_hash = ''.join(random.sample(string.uppercase,10))

		##Make a species list that may include 'TOT'
		species_list = self.species
		if len(species_list)>1: species_list.append('TOT')
			
		##Extra fields as defined in self.py that may get 
		extra_fields = ",".join(self.extra_fields)+"," if len(self.extra_fields) else ""

		###########################################
		##    Characterization              
		###########################################
		catches = ','.join(['%s_%s/1000 AS %s'%(specie,self.extract_char_catches,specie) for specie in species_list])
			
		##Limit extract to fishstocks in self.py, otherwise get (a) stat areas in other Fishstocks (b) stat areas for other species (e.g. rock lobster) that are fished in the same trips
		areas = []
		for fishstocks in self.fishstocks.values():
			for fishstock in fishstocks: 
				candidates = self.db.Values('''SELECT stat FROM qmastats WHERE qma=='%s';'''%fishstock)
				for area in candidates:
					if area not in areas: areas.append(area)
		area_clause = '''area IN (%s) AND /* Only include data in stat areas relevant to this Fishstock */'''%(','.join([repr(item) for item in areas]))
			
		self.db.Script('''
		DROP TABLE IF EXISTS events;
		CREATE TABLE events AS
		SELECT 
			fishing_year AS year, 
			CAST (strftime('%%m',start_datetime) AS INTEGER) AS month, 
			mangle(vessel_key,'%s') AS vessel,
			primary_method AS method,
			target_species AS target,
			start_stats_area_code AS area,
			form_type AS form,
			
			strftime('%%H',start_datetime)+strftime('%%M',start_datetime)/60 AS time,
			effort_depth AS depth,
			effort_height AS height,
			effort_width AS width,
			effort_length AS length,
			effort_speed AS speed,
			surface_temp AS temp,
			bottom_depth AS bottom,
			trunc_start_lat AS lat,
			trunc_start_long AS lon,
			CAST (trunc_start_lat AS INTEGER) AS latd,
			CAST (trunc_start_long AS INTEGER) AS lond,
			CAST (trunc_start_lat/0.2 AS INTEGER) * 0.2 AS latd2,
			CAST (trunc_start_long/0.2 AS INTEGER) * 0.2 AS lond2,
			zone,
			
			fishing_duration AS duration,
			effort_num AS num,
			effort_num_2 AS num2,
			effort_total_num AS total,
			total_hook_num AS hooks,
			total_net_length AS netlength,
			
			%s
			
			%s
			
		FROM fishing_event
		WHERE  dropped IS NULL AND
			year IS NOT NULL AND year>=1990 AND year<=2009 AND
			month IS NOT NULL AND
			method IS NOT NULL AND
			target IS NOT NULL AND
			area IS NOT NULL AND %s 
			form IS NOT NULL;
			
		UPDATE events SET time=NULL WHERE time==0;

		CREATE INDEX events_year ON events(year);
		CREATE INDEX events_month ON events(month);
		CREATE INDEX events_vessel ON events(vessel);
		CREATE INDEX events_method ON events(method);
		CREATE INDEX events_target ON events(target);
		CREATE INDEX events_area ON events(area);
		CREATE INDEX events_form ON events(form);
		CREATE INDEX events_latd ON events(latd);
		CREATE INDEX events_lond ON events(lond);
		CREATE INDEX events_latd2 ON events(latd2);
		CREATE INDEX events_lond2 ON events(lond2);

		'''%(vessel_hash,extra_fields,catches,area_clause))

		self.db.Commit()

		##Check that there is actually data in events
		count = self.db.Value( '''SELECT count(*) FROM events''')
		if count==0: raise Exception('No data in events: may be because all records have been dropped in groom or allocate')

		if self.scale_to_totals:

			##Scale catches so that they match year/month/fishstock totals.
			##These totals are either (a) annual from the Plenary or (b) annual MHR data for the extract (from the 2001-02 fishing year onwards).

			##Create a list of scalings done
			self.db.Script('''
				DROP TABLE IF EXISTS scalings;
				CREATE TABLE scalings(
					species_code TEXT, 
					fishing_year INTEGER,
					total REAL,
					data REAL,
					scaler REAL
				);
			''')

			for species in species_list:
				##Don't want to do it for the 'TOT' species
				if species=='TOT': continue
				
				##A list of fishstocks to scale up to for this species is taken from self.
				fishstocks = ','.join([repr(fishstock) for fishstock in self.fishstocks[species]])
					
				for fishing_year in self.db.Values('''SELECT DISTINCT year FROM events ORDER BY year;'''):
					##Get totals from history for years <=2000-01. Get totals from MHR for years >=2001-02
					##Get in tonnes
					if fishing_year<1987: total = self.db.Value('''SELECT landings FROM history WHERE fishstock IN (%s) AND fishing_year==%s;'''%(fishstocks,fishing_year))	
					elif fishing_year<=2001: total = self.db.Value('''SELECT sum(quantity)/1000 FROM qmr WHERE fishstock IN (%s) AND fishing_year==%s;'''%(fishstocks,fishing_year))	
					else:  total = self.db.Value('''SELECT sum(quantity)/1000 FROM mhr WHERE stock_code IN (%s) AND fishing_year==%s;'''%(fishstocks,fishing_year))	
					try: total = float(total)
					except (TypeError,ValueError): total = 0
					data = self.db.Value('''SELECT sum(%s) FROM events WHERE year==%s;'''%(species,fishing_year))
					##Calculate a scaler (if total is not available then just use 1)
					if total>0 and data>0: scaler = total/data
					else: scaler = 1
					##Record in scalings TABLE
					self.db.Execute('''INSERT INTO scalings(species_code,fishing_year,total,data,scaler) VALUES(?,?,?,?,?);''',(species,fishing_year,total,data,scaler))
					##Apply scaler
					self.db.Execute('''UPDATE events SET %s=%s*%s WHERE year==%s;'''%(species,species,scaler,fishing_year))

		##For each factor (a) determine the levels in appropriate order (b) create an 'Other' level to aggregate minor levels
		criterion = 'sum(%s)'%species_list[0]#count(*)
		sql = '''SELECT %s FROM events'''%criterion
		all = self.db.Value(sql)
		try: all = float(all)
		except TypeError: raise Exception('"%s;": %s'%(sql,all))
			
		factors = []
		for factor,order,maxi in ([('year',0,1),('method',1,0.99),('target',1,0.99),('area',0,1),('zone',0,1),('form',1,0.99)]+self.extra_factors):
			sql = '''SELECT %s,%s,count(*) FROM events GROUP BY %s HAVING %s NOT NULL'''%(factor,criterion,factor,factor) ##HAVING used rather than WHERE because WHERE created some unusual behaviour
			if order: sql += ''' ORDER BY %s DESC'''%criterion
			rows = self.db.Rows(sql)
			levels = []
			cum = 0
			for level,value,count in rows:
				if value is None: value = 0
				prop = value/all
				label = str(level)
				if factor=='year': label = label[-2:]
				levels.append((level,round(prop,3),label))
				cum += prop
				if cum>maxi: break
			##Update each factor so it only contains the top levels and create an index
			if maxi<1:
				self.db.Execute('''UPDATE events SET %s='Other' WHERE %s NOT IN (%s);'''%(factor,factor,','.join([repr(str(level[0])) for level in levels])))
				levels.append(('Other',max(0,1-cum),'Other'))
			self.db.Script('''DROP INDEX IF EXISTS events_%s; CREATE INDEX events_%s ON events(%s);'''%(factor,factor,factor))
			factors.append((factor,order,levels))
		##Month requires a special order
		levels = []
		for index,month in enumerate((10,11,12,1,2,3,4,5,6,7,8,9)): 
			levels.append((month,self.db.Value('''SELECT %s FROM events WHERE month==%s;'''%(criterion,month))/all,['Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep'][index]))
		factors.insert(1,('month',0,levels))

		##Only include descriptive and effort variables which are present in at least 5% of records
		overall = self.db.Value('''SELECT count(*) FROM events''')
		variables = []
		for variable in ['time','depth','height','width','length','speed','temp','bottom','latd','lond','duration','num','num2','total','hooks','netlength']:
			count = self.db.Value('''SELECT count(%s) FROM events'''%variable)
			prop = count/float(overall)
			if prop>=0.05: variables.append(variable)
				
		##Save to a smaller database
		simple_path = 'simple.db3'
		if os.path.exists(simple_path): os.remove(simple_path)
		simple = Database(simple_path)
		simple.Execute('''ATTACH DATABASE "%s" AS original;'''%('database.db3'))
		simple.Execute('''CREATE TABLE events AS SELECT * FROM original.events;''')
				
		##Save to a schema file
		schema = file('schema.py','w')
		print>>schema, "##This file was generated by %s at %s UTC.  Do not edit."%(__file__,datetime.datetime.utcnow())
		print>>schema, 'species = [%s]'%(','.join([repr(str(item)) for item in species_list]))
		print>>schema, 'factors = ['
		for factor,order,levels in factors: 
			print>>schema, '  ("%s",%s,['%(factor,order),
			for level in levels: print>>schema, '(%s,%.3f,"%s"),'%(level[0] if type(level[0]) is int else repr(str(level[0])),level[1],level[2]),
			print>>schema, ']),'
		print>>schema, ']'
		print>>schema, 'variables = [%s]'%(','.join([repr(str(item)) for item in variables]))
			
		###########################################
		##    CPUE              
		###########################################
		species_fields = ''
		for species in species_list:
			for field in self.extract_cpue_catches:	
				species_fields += ',%s_%s'%(species,field)
			
		sql = '''
		SELECT 
			/* 
			* Record identifiers and trip identifier generated by MFish
			*/
			event_key AS event,
			version_seqno AS version,
			trip AS trip,
			
			/* 
			* Fundamental, usually essential, descriptive fields. Records missing these are not included (see WHERE clause below).
			*/
			fishing_year AS fyear, 
			CAST (strftime('%%m',start_datetime) AS INTEGER) AS month, 
			primary_method AS method,
			target_species AS target,
			start_stats_area_code AS area,
			
			/*
			* Uncomment this for mangled vessel keys
			mangle(vessel_key,'%s') AS vessel,
			*/
			vessel_key AS vessel,
			
			form_type AS form,
			
			/* 
			* Other descriptive fields.  These will often only be available for some methods or form types so expect many missing values.
			*/
			strftime('%%Y-%%m-%%d',start_datetime) AS date,
			strftime('%%H',start_datetime)+strftime('%%M',start_datetime)/60 AS time, /* Time of the day in hours. Will often be 0 (midnight) because time is not recorded */
			effort_depth AS depth,
			effort_height AS height,
			effort_width AS width,
			effort_length AS length,
			effort_speed AS speed,
			surface_temp AS temp,
			bottom_depth AS bottom,
			trunc_start_lat AS lat,
			trunc_start_long AS lon,
			
			/*
			* Derived fields added in "augmenting.py"
			*/
			inshore,
			zone,
			
			/*
			* Extra fields as defined in self.py (must exist in fishing_event
			*/
			%s
			
			/* 
			* Effort 'magnitude' fields.  These will often only be available for some methods or form types so expect many missing values.
			*/
			vessel_day AS days,
			fishing_duration AS duration,
			effort_num AS num,
			effort_num_2 AS num2,
			effort_total_num AS total,
			total_hook_num AS hooks,
			total_net_length AS netlength,
			
			/* Is this record suitable for CPUE analysis? See groom.py for when this is set */
			cpue_no AS cpueno
			
			/* 
			* Catches allocated (and auxillary information from allocation) to each fishing_event for each allocated species:
			*	_est: estimated catches for this fishing_event summed from the estimated_subcatches table
			*	_equ: landings allocated equally to all fishing_events in a trip
			*	_prop: landings allocated to fishing_events in a trip in proportion to estimated catches or effort (see below for more details)
			*	_prop_method: the method used for _prop 1=proportional to _est; 2=no _est for trip so proportional to effort_num
			*	_fishstocks: the number of fishstocks for this species that the trip landed to
			*/
			%s
			
		FROM fishing_event
		WHERE  
			dropped IS NULL AND /* Don't include records that are 'dropped' during grooming */
			fyear IS NOT NULL AND
			month IS NOT NULL AND
			method IS NOT NULL AND
			target IS NOT NULL AND
			area IS NOT NULL AND %s
			form IS NOT NULL
		'''%(vessel_hash,extra_fields,species_fields,area_clause)

		out = file('cpue.txt','w')

		##Get column names of SQL
		self.db.Execute(sql + '''LIMIT 1''')
		print>>out, '\t'.join(self.db.Fields())
				
		##Get the data
		for row in self.db.Rows(sql): print>>out, '\t'.join((str(item) if item is not None else 'NA') for item in row)
			
		##Print out a description
		out = file('cpue_desc.txt','w')
		print>>out,'''
		The SQL query used to generate this data extract is below.  This code includes comments for fields where appropriate.
		Note that most fields are just the MFish fields renamed to something more succinct.  See MFish Warehou documentation for 
		field explanations.

		%s

		'''%sql

		##Could include a suimmary of dropped and changed for CPUE
		##'''SELECT changed,count(*) FROM fishing_event GROUP BY changed;''

		self.db.Execute('''INSERT INTO status(task,done) VALUES('simplify',datetime('now'));''')
		self.db.Commit()

	def summarize(self,prefix=''):
		'''Provides a summary of the data and the error checks that were done on it.'''

		import rpy2.rpy_classic as rpy
		rpy.set_default_mode(rpy.NO_CONVERSION)
		from rpy2.rpy_classic import r as R
		import math

		import string
		import cgi

		class _Tag:
		    '''Based on http://www.elfsternberg.com/2008/05/30/dwim-html-templating-in-python/'''
		    def __init__(self, *content, **kw):
			self.data = list(content)
			self._attributes = {}
			for i in kw: self._attributes[(i == 'klass' and 'class') or (i == 'fur' and 'for') or i] = kw[i]

		    def render_content(self, content):
			if type(content) == type(""):
			    return content
			if hasattr(content, '__iter__'):
			    return string.join([self.render_content(i) for i in content], '')
			return self.render_content(str(content))

		    def __iadd__(self,value):
			     self.data.append(value)
			     return self

		    def __str__(self):
			tagname = self.__class__.__name__.lower()
			return ('<%s' % tagname +
			       (self._attributes and ' ' or '') +
			       string.join(['%s="%s"' %
					    (a, cgi.escape(str(self._attributes[a])))
					    for a in self._attributes], ' ') +
			       '>' +
			       self.render_content(self.data) +
			       '</%s>\n' % tagname)

		class Html:
			def __init__(self,filename):
				self.out = file(filename,'w')
					
			def __iadd__(self,value):
				self.out.write(str(value))
				self.out.flush()
				return self
				
		class Head(_Tag): pass
		class Body(_Tag): pass
		class Title(_Tag): pass
		class Div(_Tag): pass
			
		class H(_Tag):
			def __init__(self, content, **kw):
				_Tag.__init__(self,'%s%s'%(prefix,content),**kw)
		class H1(H): pass
		class H2(H): pass
		class H3(H): pass
		class H4(H): pass
			
		class P(_Tag): pass
		class Br(_Tag): pass	
		class Img(_Tag): pass
		class Table(_Tag): pass
		class Caption(_Tag): pass	
		class TR(_Tag): pass
		class TD(_Tag): pass	
		class TH(_Tag): pass		

		class TableIndexHolder:
			TableIndex = 0
		def Tabulate(caption,header,rows):
			TableIndexHolder.TableIndex += 1
			div = Div(P('Table %s%s: %s'%(prefix,TableIndexHolder.TableIndex,caption),klass='caption'),klass='table')
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

		class FigureIndexHolder:
			FigureIndex = 0
		def Figurate(filename,caption="No caption defined"):
			FigureIndexHolder.FigureIndex += 1
			return Div(Img(src=filename),P('Figure %s%s: %s'%(prefix,FigureIndexHolder.FigureIndex,caption),klass='caption'),klass='figure')
			
		def Quantiles(values,at=[0.05,0.5,0.95]):
			'''Calculate quantiles of a vector'''
			qs = []
			for p in at: 
				index = int(len(values)*p)-1
				qs.append(values[index])
			return qs
			
		def Filename(filename):
			'''Create a valid filename from a string'''
			return str(filename.replace(' ','_').replace('/','di').replace('>','gt').replace('>=','ge').replace('<','lt').replace('<=','le').replace('"','').replace("'",''))
			
		def Histogram(table,field,where='',transform='',xlab='',ylab='',lines=[],caption='No caption defined'):
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
				
			p1,p5,median,p95,p99 = Quantiles(values,at=[0.01,0.05,0.5,0.95,0.99])
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
				
			filename = Filename('%s %s %s hist.png'%(table,field,where))
			R.png(filename,600,400)
			R.hist(values,breaks=30,main='',xlab=xlab,ylab=ylab,col='grey')
			R.legend("topright",legend=['N=%i'%n,'P5=%.2f'%p5,'Med=%.2f'%median,'GM=%.2f'%geomean,'P95=%.2f'%p95],bty='n')
			for line in lines: R.abline(v=line,lty=2)
			R.dev_off()
			
			return Figurate(filename,caption)
			
		def Scatterplot(table,x,y,where='',transform='',xlab='',ylab='',lines=[],caption='No caption defined'):
			sql = '''SELECT %s,%s FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL'''%(x,y,table,x,y)
			wheres = []
			if 'log' in transform: wheres.append('''%s>0 AND %s>0'''%(x,y))
			if len(where)>0: wheres.append(where)
			if len(wheres)>0: sql += ''' AND ''' + ''' AND '''.join(wheres)
			rows = self.db.Rows(sql)
				
			filename = Filename('%s %s %s %s %s scat.png'%(table,x,y,transform,where))
			
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
				R.plot([x for x,y in rows],[y for x,y in rows],xlab=xlab,ylab=ylab)
				for line in lines: R.abline(a=line[0],b=line[1],lty=2)
			R.dev_off()
			
			return Figurate(filename,caption)	

		fishing_years = range(1990,datetime.datetime.now().year-1)
		
		#!todo Not working because fishing_year not yet defined - just move to sumarize
		#report += Tabulate('Alternative catch allocations for %s by fishing year'%species,('Fishing year','Estimated','Equal','Proportional'),self.db.Rows('''SELECT fishing_year,sum(%s_est)/1000,sum(%s_equ)/1000,sum(%s_prop)/1000 FROM fishing_event GROUP BY fishing_year;'''%(species,species,species)))
		#report += Tabulate('Proportional allocation methods for %s by fishing year'%species,('Fishing year','Using estimated','Using effort','Equal'),self.db.Rows('''SELECT fishing_year,sum(%s_prop_method==1),sum(%s_prop_method=2),sum(%s_prop_method==3) FROM fishing_event GROUP BY fishing_year;'''%(species,species,species)))
			
		##BPT and MPT
		##The is a table called "ce_event_assoc_object" which records the vessel_key of the other vessel in a pair trawling pair.
		##This would be required to do better summaries of this.
		##Summarize trips that had estimated catch but no landings by year
		##Problem arises in estimated_catches.
		
		if not os.path.exists('summarize'): os.mkdir('summarize')
		report = Html('summarize/index.html')
		report += '<html>'
		report += Head('''
			<style type="text/css">
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
			</style>
		''')
		report += '<body>'
		
		report += P('''APPENDIX %s. DATA EXTRACTION, GROOMING AND ALLOCATION'''%prefix,klass='title')
		
		report += P('''This appendix summarises the extraction, grooming and allocation of landings for the data set.
		The first section describes the data extract obtained from the Ministry of Fisheries. There are then sections for each database table
		which describe the error checks done. Each error check is referred to using a three letter code and the section
		for each table usually begins with a table that provides an overview of the checks done.
		Many of the error checks are based on those described in Starr (2010) and in those cases the relevant paragraph is denoted (e.g. Starr D.1.9).
		Next is a section describing the allocation of landings to fishing events. Finally, there is a section in which summarises the grooming and allocation done.''')

		report += H1('''1. Data extract''')
		
		p =  P('''Catch and effort data was obtained for fishing trips that occurred between %s and %s, and that '''%(self.begin,self.end))
		species = ', '.join(self.species)
		fishstocks=', '.join([', '.join(fishstocks) for fishstocks in self.fishstocks.values()])
		if fishstocks: p += '''landed to %s'''%(fishstocks)
		criteria = []
		if len(self.statareas): criteria.append('''<li>were in statistical area(s) %s,</li>'''%(', '.join(self.statareas)))
		if len(self.methods): criteria.append('''<li>used method(s) %s,</li>'''%(', '.join(self.methods)))
		if len(self.targets): criteria.append('''<li>targeted species %s</li>'''%(', '.join(self.targets)))
		if hasattr(self,'targets_not') and len(self.targets_not): criteria.append('''<li>did not target species %s</li>'''%(', '.join(self.targets_not)))
		if len(criteria)>0: p += ''', or had fishing events that:<ul>%s</ul>\n''' %('<b>and</b>'.join(criteria))
		report += p
		
		p = P('''For those trips we would obtained all effort data as well as landings and estimated catch data for the species %s.  
			In addition, monthly harvest return (MHR) data for all available months for the above fishstocks was obtained by month, client and Fishstock.'''%species)
		if self.request_extra_notes: p += '''We also requested that:"'''+self.request_extra_notes+'"'
		report += p
		
		##trip_details
		report += H1('''2. Trip details table''')
		report += P('''The table <i>trip_details</i> contains one record for each trip number (assigned by MFish) with fields for vessel, and start and end dates and times.''')

		report += H2('''2.1 Trip date check (TRD)''')
		report += P('''Starr D.1.8 describes a method for calculating the "best date" for a trip.  This involves determining the length (in days) of each trip,  
		calculating the 95th percentile of trip length for each type of form
		and using that as the basis for determining if the trip end date is likely.  The field <i>best_date</i> which is created is not actually used for characterization and CPUE analyses 
		because the field <i>fishing_event.start_datetime</i> is used instead.  However, the following summaries may indicated potential problems with the asignment of a trip number''')
		report += Histogram('trip_details','trip_length',xlab='Trip length (days)',ylab='Trips',
					caption='Frequency distibution of trip length for all trips in dataset.')
		
		##estimated_subcatch
		report += H1('''3. Estimated catch table''')

		num = self.db.Value('''SELECT count(species) FROM estimated_subcatch_CTN;''')
		if num>0:
			report += H2('''3.1 Estimated catch entered as weight instead of numbers check (CTN)''')
			report += P('''For a few species, estimated catch should be recorded in numbers rather than weights.  This check is designed to find and change those records 
			where the weight was recorded instead of numbers.  This is done by comparing the estimated catch with the landings for each trip for each species. If the ratio of landings to estimated catch
			is less than a specified threshold then the estimated catch is assumed to have been mis-reported as a catch weight and is adjusted by diving by a specified average weight.''')

			for species in self.db.Values('''SELECT species FROM estimated_subcatch_CTN;'''):
				report += H3(species)
				
				values = self.db.Values('''SELECT sum_green_weight/sum_catch_weight FROM estimated_subcatch_CTN_%s WHERE sum_catch_weight>0 AND sum_green_weight>0;'''%(species))
				values.sort()
				p1,p5,median,p95,p99 = Quantiles(values,at=[0.01,0.05,0.5,0.95,0.99])
				geomean = math.exp(sum([math.log(value) for value in values])/len(values))
				
				filename = str('summarize/estimated_subcatch_CTN_%s_hist.png'%(species))
				R.png(filename,600,400)
				R.hist([math.log10(value) for value in values if value>=p1 and value<=p99],breaks=30,main='',xlab='log10 (Landed green weight / Estimated catch)',ylab='Trips',col='grey')
				R.legend("topright",legend=['5th percentile = %.2f'%p5,'Median = %.2f'%median,'Geometric mean = %.2f'%geomean,'95th percentile = %.2f'%p95],bty='n')
				if self.groom_estimated_subcatch_catch is not None: 
					minimum,average = self.groom_estimated_subcatch_catch[species]
					R.abline(v=(math.log10(minimum),math.log10(average)),lty=(2,3),col=('red','blue'))
					R.legend("topleft",legend=['Threshold = %s'%minimum,'Average = %s'%average],lty=(2,3),col=('red','blue'),bty='n')
				R.dev_off()
				
				report += Figurate(src=filename) 
				
		##landing
		report += H1('''4. Landings table''')
		report += P('''In the following summaries of errors in the landings table, the reported landing weight (in tonnes) is for ALL species combined (unless explicitly by species or fishstock)''')

		rows = self.db.Rows('''SELECT dropped,'Dropped',count(*),sum(green_weight)/1000 FROM landing WHERE dropped IS NOT NULL GROUP by dropped 
				UNION SELECT changed,'Changed',count(*),sum(green_weight)/1000 FROM landing WHERE changed IS NOT NULL GROUP BY changed;''')
		##Sort according to order they are addressed below
		rows = sorted(rows,key=lambda row: [].index(row[0][:3]))
		report += Tabulate("Summary of error checks on landing table",('Error','Action','Records','Landings (t)'),rows)

		report += H2('''4.1 Drop if landing date/time missing or in future (DAM & DAF)''')
		report += P('''Records with either of these errors are dopped because this date is required for scaling catches.''')
		report += Tabulate('DAF errors by landing_datetime date',('Month','Records','Landings (t)'),
			self.db.Rows('''SELECT strftime('%Y-%m',landing_datetime),count(*),sum(green_weight)/1000 FROM landing WHERE dropped=='DAF' GROUP BY strftime('%Y-%m',landing_datetime)'''))

		report += H2('''4.2 Drop if invalid or unused destination codes (DES)''')
		report += P('''Starr D.1.1 suggests dropping records with any destination_type that is not in the list 'A','C','E','F','H','L','O','S','U','W'.''')
		report += Tabulate('''DES errors by destination_type''',('Code','Records','Landings (t)'),
			self.db.Rows('''SELECT destination_type,count(*),sum(green_weight)/1000 FROM landing WHERE dropped=='DES' GROUP BY destination_type;'''))
		codes = self.db.Values('''SELECT DISTINCT destination_type FROM landing  WHERE destination_type IS NOT NULL AND (dropped IS NULL OR dropped=='DES');''')
		rows = []
		for fy in fishing_years:
			row = [fy]
			for code in codes: row.append(self.db.Value('''SELECT sum(green_weight)/1000 FROM landing WHERE destination_type==? AND fishing_year==? AND (dropped IS NULL OR dropped=='DES');''',(str(code),fy)))
			rows.append(row)
		report += Tabulate('''Landings (t) by <i>destination_type</i> and <i>fishing_year</i>. This includes records dropped by the "DES" check but not those 
							dropped by other checks.''',['Fishing year']+codes,rows)

		report += H2('''4.2 Change common invalid state code (SCR)''')
		report += P('''Starr D.1.4 suggests "Find commonly entered invalid state codes and replace with correct state code"''')
		report += Tabulate('''SCR errors by original and replacement state_code''',('Original','Replacement','Records','Landings (t)'),
			self.db.Rows('''SELECT orig,new,count(*),sum(green_weight)/1000 FROM landing_changes,landing WHERE landing_changes.id = landing.id AND code=='SCR' GROUP BY orig,new;'''))
		codes = self.db.Values('''SELECT DISTINCT state_code FROM landing  WHERE state_code IS NOT NULL AND dropped IS NULL;''')
		rows = []
		for fy in fishing_years:
			row = [fy]
			for code in codes: row.append(self.db.Value('''SELECT sum(green_weight)/1000 FROM landing WHERE state_code==? AND fishing_year==? AND dropped IS NULL;''',(str(code),fy)))
			rows.append(row)
		report += Tabulate('''Landings (t) by <i>state_code</i> and <i>fishing_year</i>.''',
						['Fishing year']+codes,rows)
			
		report += H2('''4.3 Change if still invalid state code (STI)''')
		report += P('''Set state_code to NULL if still invalid (ie not changed in SCR).  Valid state codes are: 'GRE','GUT','HGU','DRE','FIL','SKF','USK','SUR','SUR','TSK','TRF','DSC','DVC','MEA','SCT','RLT','TEN','FIN',
			'LIV','MKF','MGU','HGT','HGF','GGU','SHU','ROE','HDS','HET','FIT','SHF','MBS','MBH','MEB','FLP','BEA','LIB',
			'CHK','LUG','SWB','WIN','OIL','TNB','GBP'.''')
		report += Tabulate('''STI errors by original and replacement state_code''',('Original','Records','Landings (t)'),
			self.db.Rows('''SELECT orig,count(*),sum(green_weight)/1000 FROM landing_changes,landing WHERE landing_changes.id = landing.id AND code=='STI' GROUP BY orig;'''))
			
		report += H2('''4.4 Drop if no matching stat area on trip for a fishstock (FSM)''')
		report += P('''Apart from being inconsistent, these trips need to be dropped because landings can not be allocated properly''')
		report += Tabulate('FSM errors by fishstock',('Fishstock','Records','Landings (t)'),
			self.db.Rows('''SELECT fishstock_code,count(*),sum(green_weight)/1000 FROM landing WHERE dropped=='FSM' GROUP BY fishstock_code'''))
		#report += Tabulate('For trips with FSM errors the stat area recorded by fishstock (limited to 100)',('Fishstock','Stat area','Trips'),
		#	self.db.Rows('''SELECT fishstock_code,start_stats_area_code,count(*) FROM landing_FSM GROUP BY fishstock_code,start_stats_area_code ORDER BY count(*) DESC LIMIT 100;'''))
		#report += Tabulate('For trips with FSM errors summary of the port of landing (limited to 250)',('Fishstock','Stat area','Port','Count'),
		#	self.db.Rows('''SELECT lfsm.fishstock_code,start_stats_area_code,landing_name,count(*) FROM landing_FSM AS lfsm LEFT JOIN landing USING (trip) 
		#		GROUP BY lfsm.fishstock_code,start_stats_area_code,landing_name ORDER BY count(*) DESC LIMIT 250;'''))
		if 0:
			for row in self.db.Rows('''SELECT species,start_stats_area_code,fishstock_code,count(*) FROM fishing_event_FSM GROUP BY species,start_stats_area_code,fishstock_code ORDER BY count(*) DESC;'''): print row
				
			for row in self.db.Rows('''SELECT fishstock_code,landing_name,count(*),sum(green_weight) FROM landing WHERE dropped=='FSM' GROUP BY fishstock_code,landing_name ORDER BY sum(green_weight) DESC;'''):
				print row

		report += H2('''4.5 Drop duplicates (DUP)''')
		report += P('''Starr D.1.2 suggests "Look for duplicate landings on multiple (CELR and CLR) forms. Keep only a single version if determined that the records are duplicated".
		For this implementation, duplicates are those where the following field are exactly the same: vessel_key, landing_datetime, fishstock_code, state_code, destination_type, unit_type, unit_num, unit_weight, green_weight. 
		If supliocates are found then drop all records other than the CELR record.''')
		report += Tabulate('''DUP errors by fishstock_code,state_code,destination_type''',('Fishstock','State','Destination','Records','Landings (t)'),
			self.db.Rows('''SELECT fishstock_code,state_code,destination_type,count(*),sum(green_weight)/1000 FROM landing WHERE dropped=='DUP' GROUP BY fishstock_code,state_code,destination_type;'''))
			
		report += H2('''4.6 Change missing conversion factors (COM)''')
		report += P('''Starr D.1.3 suggests "Find missing conversion factor fields and insert correct value for relevant state code and fishing year.Missing fields can be inferred from the median of the non-missing fields."
		In this implementation we replace missing values with the median over all fishing_years for that state_code.''')
		report += Tabulate('''COM errors by state_code and replacement conversion factor''',('Sate','Replacement','Records','Landings (t)'),
			self.db.Rows('''SELECT state_code,new,count(*),sum(green_weight)/1000 FROM landing_changes,landing WHERE landing_changes.id = landing.id AND code=='COM' GROUP BY state_code,new;'''))
			
		report += H2('''4.7 Examine for changes in conversion factor (COV)''')
		report += P('''The following table shows state codes that have more than one conversion factor recorded.''')
		report += Tabulate('''Conversion factors used for each state code''',('Species','State','Conversion factor','Records','Landings (t)'),
			self.db.Rows('''SELECT species_code,state_code,conv_factor,count(*),sum(green_weight)/1000 FROM landing 
			WHERE species_code IS NOT NULL AND state_code IS NOT NULL AND conv_factor IS NOT NULL GROUP BY species_code,state_code,conv_factor;'''))

		##Table of median conversion factor by fishing_year and state_code
		states = self.db.Values('''SELECT state_code FROM landing WHERE state_code IS NOT NULL  AND conv_factor IS NOT NULL GROUP BY state_code HAVING count(*)>=100 ORDER BY count(*) DESC;''')
		medians = self.db.Rows('''SELECT fishing_year,state_code,median(conv_factor) FROM landing WHERE state_code IS NOT NULL  AND conv_factor IS NOT NULL GROUP BY fishing_year,state_code;''')
		medians = dict(zip(['%s-%s'%(y,s) for y,s,m in medians],[m for y,s,m in medians]))
		rows = []
		for fy in fishing_years:
			row = [fy]
			for state in states:
				try: median = medians['%s-%s'%(fy,state)]
				except: median = ''
				row.append(median)
			rows.append(row)
		report += Tabulate('The median conversion factor in each fishing year by processed state (for states having at least 100 records)',['Fishing year']+states,rows)

		report += H2('''4.8 Drop records for 'bits' of fish (STD)''')
		report += P('''Starr D.1.6 suggests "Drop landings where state code is FIN,FLP,SHF or ROE and there is more than one record for the trip/Fishstock combination."''')
		report += Tabulate('''STD errors by state_code''',('State','Records','Landings (t)'),
			self.db.Rows('''SELECT state_code,count(*),sum(green_weight)/1000 FROM landing WHERE dropped=='STD' GROUP BY state_code;'''))
			
		report += H2('''4.9 Check greenweight calculations (GRC,GRO,GRM): ''')
		report += P('''Starr D.1.7 suggest "Check for missing data in the unit_num and unit_weight fields. Drop records where greenweight=0 or =NULL and either unit_num and unit_weight is missing.Missing greenweight can be estimated". In this implementation: 
		<ul>
			<li>GRC = conv_factor*unit_num*unit_weight WHERE conv_factor IS NOT NULL</li>
			<li>GRO = unit_num*unit_weight WHERE conv_factor IS NULL</li>
			<li>GRM = drop records where green_weight is still zero or null</li>
		</ul>
		See summary table above for record and landing asscoiated with each.
		''')
			
		report += H2('''4.10 Green weight range check (GRR)''')
		for index,species in enumerate(self.db.Values('''SELECT DISTINCT species FROM landing_GRR;''')):

			report += H3('4.10.%i %s'%(index+1,species))
			
			##Get first method for this species (the same estimated & landing info is in each "landing_GRR_<species>_<method>" table so it does not matter which one it is)
			method = self.db.Value('''SELECT method FROM landing_GRR WHERE species==? LIMIT 1;''',[species])
			##Get the estimated & landing info from this table
			row = self.db.Rows('''SELECT trip, sum_green_weight, sum_calc_weight, sum_est_weight, ratio_green_calc, ratio_green_est FROM landing_GRR_%s_%s;'''%(species,method))
			
			##Histograms of catch/landings ratios
			for field,label in [
				('ratio_green_calc','Landed green weight/Landed calculated weight'),
				('ratio_green_est','Landed green weight/Estimated catch'),
			]: report += Histogram('landing_GRR_%s_%s'%(species,method),field,xlab=label,lines=(0.75,1.33,4),
							caption="Frequency distribution of %s for species %s and method %s"%(label.lower(),species,method))
			
			for method,landings_threshold,cpue_threshold in self.db.Rows('''SELECT method,landings_threshold,cpue_threshold FROM landing_GRR WHERE species=='%s';'''%species):
			
				table = 'landing_GRR_%s_%s'%(species,method)
				
				report += H4(method)
				report += P('Landings threshold: %.2f<br>CPUE threshold: %.2f'%(landings_threshold,cpue_threshold))
				
				report += Histogram(table,'sum_green_weight',where='sum_effort>0',lines=[landings_threshold],transform='log10',xlab='Landings',ylab='Trips',
					caption='Landings for trips that used %s and landed %s.'%(method,species))
				report += Histogram(table,'cpue',where='ok=1',lines=[cpue_threshold],transform='log10',xlab='CPUE',ylab='Trips',
					caption='CPUE for trips that used %s and landed %s and which had the ratio of green_weight to estimated_weight between 0.75 and 1.33.'%(method,species))
				
				##Summarise those trips that were dropped
				dropped = self.db.Rows('''SELECT * FROM %s WHERE dropped==1;'''%table)
				if len(dropped)>0: report += Tabulate('Details of trips dropped',[col[0] for col in self.db.Cursor.description],dropped)
				else: report += 'No trips dropped by this check.'

		##fishing_event
		report += H1('''5. Fishing event table''')

		##Need to decide on a species that is used as a basis for reporting estimated catches.  Use the first of difinition.species
		species = self.species[0]

		report += Tabulate('Summary of error checks on fishing_event table',('Error','Action','Records','Catch (t)'),
			self.db.Rows('''SELECT dropped,'Dropped',count(*),sum(%(species)s_est)/1000 FROM fishing_event WHERE dropped IS NOT NULL GROUP by dropped 
				UNION SELECT changed,'Changed',count(*),sum(%(species)s_est)/1000 FROM fishing_event WHERE changed IS NOT NULL GROUP BY changed;'''%locals()))

		report += H2('''5.1 Missing fishing method (PMR & PRM)''')
		report += P('''Starr D.2.1 suggests "Look for missing method codes by trip. a) drop the entire trip if more than one method was used for the trip; b) if a single method trip, insert the method into the missing field."''')
		report += Tabulate('''PMR errors by replacement primary_method''',('Method','Records','Catch (t)'),
			self.db.Rows('''SELECT primary_method,count(*),sum(%(species)s_est)/1000 FROM fishing_event WHERE changed=='PMR' GROUP BY primary_method;'''%locals()))
			
		report += H2('''5.2 Missing statistical area (SAR & SAM)''')
		report += P('''Starr D.2.2 suggests "Search for missing statistical area fields. a) drop the entire trip if all statistical areas are missing in the trip; b) substitute the 'predominant' (most frequent) statistical area for the trip for trips which report the statistical area fished in other records.""''')
		report += Tabulate('''SAR errors by replacement start_stats_area_code''',('Statistical area','Records','Catch (t)'),
			self.db.Rows('''SELECT start_stats_area_code,count(*),sum(%(species)s_est)/1000 FROM fishing_event WHERE changed=='SAR' GROUP BY start_stats_area_code;'''%locals()))
			
		report += H2('''5.3 Missing target species (TAR & TAM)''')
		report += P('''Search for missing target species fields. a) drop the entire trip if all target species are missing in the trip; b) substitute the 'predominant' (most frequent) target species for the trip for trips which report the target species in other records''')
		report += Tabulate('''TAR errors by replacement target_species''',('Statistical area','Records','Catch (t)'),
			self.db.Rows('''SELECT target_species,count(*),sum(%(species)s_est)/1000 FROM fishing_event WHERE changed=='TAR' GROUP BY target_species;'''%locals()))
			
		report += H2('''5.4 Positions(lat/lon) outside of statistical area (POS)''')
		report += P('''Set lat or lon to null if they are outside of the bounding box of the statistical area."''')
			
		report += H2('''5.5 Change outlier effort values (EFF)''')
		report += P('''See Starr D.2.4 "Operate grooming procedure on effort fields by method of capture and form type to truncate outlier values."''')
		report += Tabulate('''EFF changes by effor field, form_type and primary_method''',('Effort field','Form type','Method','Records not null','Records not null (%)','Min','10th percentile','Median','90th percentile','Max','Floor','Multiplier','Lower','Upper','Substitutions (%)'),
			self.db.Rows('''SELECT * FROM fishing_event_EFF;'''))
			
		report += H1('''6. Allocation of landings to fishing events''')

		report += P('''Landings are allocated to fishing events following the Starr (2010) method. Prior to allocation, records in the fishing_event table are dropped using two checks STR and OTH''')

		report += H2('''6.1. Statistical areas associated with other Fishstocks (OTH)''')
		report += P('''Event in an external statistical area''')
		report += Tabulate('OTH:Records dropped',('Statistical area','Records'),self.db.Rows('''SELECT start_stats_area_code,count(*) FROM fishing_event WHERE dropped=="OTH" GROUP BY start_stats_area_code;'''))

		report += H2('''6.2 Straddling statistical areas with landings of other Fishstocks (STR)''')
		report += P('''Trip fished in a straddling statistical area and landed to a fishstock other than the ones being allocated for''')
		for species in self.db.Values('''SELECT DISTINCT species FROM fishing_event_STR'''):
			report += H3(species)
			report += Tabulate('''Numbers of trips that fished in straddling statistical areas and that landed to another Fishstock. None means that one of the Fishstocks of interest (i.e. none 'other')''',('Statistical area','Other Fishstock','Trips'),
				self.db.Rows('''SELECT stat,other,count(*) FROM fishing_event_STR GROUP BY stat,other'''))
		report += Tabulate('STR:Records dropped',('Code','Records','Landings (t)'),self.db.Rows('''SELECT dropped,count(*),sum(green_weight)/1000 FROM landing WHERE dropped LIKE "STR %" GROUP BY dropped;'''))

		report += H1('''6.3 Estimated versus allocated''')
		for species in self.species:
			report += H2(species)
			report += Histogram('fishing_event','%s_est/%s_prop'%(species,species),where='dropped IS NULL',ylab='Events',xlab='Estimated catches/Allocated landings')
			report += Scatterplot('fishing_event','%s_est'%species,'%s_prop'%species,where='dropped IS NULL',xlab='Estimated catches',ylab='Allocated landings',lines=[(0,1)])
			report += Scatterplot('fishing_event','%s_est'%species,'%s_prop'%species,transform='log10',where='dropped IS NULL',xlab='Estimated catches',ylab='Allocated landings',lines=[(0,1)])
					
		report += H1('''8. Summary of grooming and allocation''')
		##Table of landings dropped by check
		for fishstock in self.fishstocks[self.species[0]]:
			species = fishstock[:3]
			values = {}
			checks = ['DAM','DES','DUP','STD', 'GRR','FSM','STR']
			values_check = self.db.Rows('''SELECT fishing_year,substr(dropped,1,3),sum(green_weight)/1000 FROM landing WHERE fishstock_code=='%s' GROUP BY fishing_year,substr(dropped,1,3);'''%fishstock)
			values.update(dict(zip(['%s-%s'%(y,s) for y,s,m in values_check],[m for y,s,m in values_check])))
			rows = []
			for fy in fishing_years:
				row = [
					fy,
					self.db.Value('''SELECT landings FROM history WHERE fishstock=='%s' AND fishing_year==%s;'''%(fishstock,fy)),
					self.db.Value('''SELECT sum(quantity)/1000 FROM qmr WHERE fishstock=='%s' AND fishing_year==%s;'''%(fishstock,fy)),
					self.db.Value('''SELECT sum(quantity)/1000 FROM mhr WHERE stock_code=='%s' AND fishing_year==%s;'''%(fishstock,fy)),
					self.db.Value('''SELECT sum(green_weight)/1000 FROM landing WHERE fishstock_code=='%s' AND fishing_year==%s;'''%(fishstock,fy)),
				]
				for check in checks:
					try: value = round(values['%s-%s'%(fy,check)],2)
					except KeyError: value = '-'
					except TypeError: value = '-' ##Value is None
					row.append(value)
				row.extend([
					self.db.Value('''SELECT sum(green_weight)/1000 FROM landing WHERE dropped IS NULL AND fishstock_code=='%s' AND fishing_year==%s;'''%(fishstock,fy)),
					self.db.Value('''SELECT sum(%s_est)/1000 FROM fishing_event WHERE dropped IS NULL AND trip IN (SELECT trip FROM landing WHERE fishstock_code=='%s' AND fishing_year==%s);'''%(species,fishstock,fy)),
					self.db.Value('''SELECT sum(%s_prop)/1000 FROM fishing_event WHERE dropped IS NULL AND trip IN (SELECT trip FROM landing WHERE fishstock_code=='%s' AND fishing_year==%s);'''%(species,fishstock,fy)),
					self.db.Value('''SELECT sum(%s_prop)/1000 FROM fishing_event WHERE dropped IS NULL AND start_stats_area_code IN (SELECT stat FROM qmastats WHERE qma=="%s") AND fishing_year==%s;'''%(species,fishstock,fy))
				])
				rows.append(row)
			report += Tabulate('Landings (t) dropped by check for %s'%fishstock,['Fishing year','Published','QMR','MHR','Original(all data)']+checks+['Remaining(dropped==NULL)','Fishing events (est)', 'Fishing events (alloc)','Fishing events (alloc,separ)'],rows)

		report += '</body></html>'

		self.db.Execute('''INSERT INTO status(task,done) VALUES('summarize',datetime('now'));''')
		self.db.Commit()
