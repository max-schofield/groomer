from check import *

class LASTA(CHSTA):
	table = 'landing'
	def do(self):
		CHSTA.do(self)
		self.db.Alter('''ALTER TABLE landing ADD COLUMN fishing_year INTEGER;''') 
		self.db.Execute('''UPDATE landing SET fishing_year=strftime('%Y',landing_datetime);''')
		self.db.Execute('''UPDATE landing SET fishing_year=fishing_year+1 WHERE strftime('%m',landing_datetime)>="10";''')
		self.db.Execute('''CREATE INDEX IF NOT EXISTS landing_fishing_year ON landing(fishing_year);''')
		
		self.db.Alter('''ALTER TABLE landing ADD COLUMN dropped BOOLEAN;''') 
		self.db.Execute('''UPDATE landing SET dropped=NULL;''')
		
class LA(Check):
	table = 'landing'
	
	##Whether of not landings whould be 'dropped' if this check fails
	drop = False
	
	def flag(self,table=None,clause=None,details=None):
		##In addition to Check.flag consider drop
		Check.flag(self,table,clause,details)
		if self.drop: self.db.Execute('''UPDATE landing SET dropped=1 WHERE flags LIKE '%%%s%%';'''%self.code())
	
	def change(self,table=None,clause=None,column=None,value=NotDefined,expr=None,details=None):
		##Check that don't have drop==Flase and calling change
		assert self.drop==False
		Check.change(self,table,clause,column,value,expr,details)

class LADAM(LA):
	brief = 'Date is missing'
	desc = '''
		The landing date/time can be missing. This check flags those records but no attempt is made to impute the date/time.
	'''
	column = 'landing_datetime'
	clause = '''landing_datetime IS NULL'''
	
class LADAF(LA):
	brief = 'Date is in the future'
	desc = '''
		The landing date/time can be in the future. This check flags those records but no attempt is made to correct date/time.
	'''
	column = 'landing_datetime'
	clause = '''landing_datetime>datetime('now')'''
	
class LADTI(LA):
	brief = 'Destination type is invalid'
	desc = '''This check removes invalid destination codes based on the following list of valid from the Warehou documentation v8.0:
		<ul>
			<li>A Accidental loss 
			<li>B Stored as Bait 
			<li>C Disposed to crown 
			<li>D Discarded (NON-ITQ) 
			<li>E Eaten 
			<li>F Section 111 Recreational Catch 
			<li>H Loss from Holding Pot 
			<li>L Landed in NZ (to LFR) 
			<li>M QMS returned to sea (Part 6A) 
			<li>O Conveyed outside NZ 
			<li>P Holding receptacle in the water 
			<li>Q Holding receptacle on land 
			<li>R Retained on board 
			<li>S Seized by crown
			<li>T Transfer to another vessel
			<li>U Used for Bait
			<li>W Sold at wharf
			<li>X QMS returned to sea, except 6A
		</ul>'''
	column = 'destination_type'
	list = ('A','B','C','D','E','F','H','L','M','O','P','Q','R','S','T','U','W','X')
	clause =  '''destination_type NOT IN %s'''%repr(list)

	def summarise(self):
		div = Div()
		div += P('The following tables summarise records in the <i>landing</i> table by <i>destination_type</i>.')
                
        #By code across years
		rows = []
		for species in self.dataset.species:
			rows += self.db.Rows('''SELECT 
						species_code,
						destination_type,
						count(*),
						sum(green_weight)/1000,
						sum(green_weight)/(SELECT sum(green_weight) FROM landing WHERE species_code=='%s')*100
					FROM landing 
					WHERE species_code=='%s'
					GROUP BY destination_type 
					ORDER BY count(*) DESC;'''%(species,species))
		##Add in a column indicating if a valid code or not
		rows = [list(row[0:2])+['Y' if row[1] in self.list else 'N']+list(row[2:]) for row in rows]
		#for row in rows: print row
		div += FARTable(
			'''Records in the <i>landing</i> table by destination_type''',
			('Species','Destination','Valid','Records','Landings (t)','Landings (%)'),
			rows
		)
                
        # By code and year for each species
		div += FARTable(
			'''Records in the <i>landing</i> table by species_code, destination_type and fishing_year for the top five species and top five destination codes.''',
			('Species','Destination','Fishing year','Records','Landings (t)'),
			self.db.Rows('''
                SELECT 
                    species_code,
                    destination_type,
                    fishing_year,
                    count(*),
                    sum(green_weight)/1000
                FROM landing
                WHERE 
                	species_code IN (SELECT species_code FROM landing GROUP BY species_code ORDER BY sum(green_weight) DESC LIMIT 5) AND
                	destination_type IN (SELECT destination_type FROM landing GROUP BY destination_type ORDER BY sum(green_weight) DESC LIMIT 5)
                GROUP BY species_code,destination_type,fishing_year;
             ''')
		)
                
                
		return div

class LAFLA(LA):
	brief = 'Fishstock code is for a flatfish species'
	desc = '''Occaisionaly flatfish landings are recorded using the species specific code (e.g. GFL) instead of the general flatfish code (FLA).
			This check is done early so that total flatfish landings are checked in subsequent landings checks.'''
	column = 'fishstock_code'
	clause =  "substr(fishstock_code,1,3) IN ('BFL', 'BLF', 'BRI', 'ESO', 'FLO', 'GFL', 'LSO', 'SFL', 'SOL', 'TUR', 'WIT', 'YBF', 'BOT', 'GBL', 'MAN', 'SLS', 'SDF')"
	expr = "'FLA'||substr(fishstock_code,4,5)"

	
class LADTH(LA):
	drop = True
	brief = 'Landings held'
	desc = '''Some destination codes relate to catches that are retained by the vessel, either on board or in holding receptacles.
	These "non-terminal" landings may be double counted if they are retained. This check therefore flags all landing records where the 
	destination_type is either 'P' (Holding receptacle in the water), 'Q' (Holding receptacle on land), or 'R' (Retained on board). 
	These catches should be landed on a subsequent trip and whilst that will inflate the cath-per-unit-effort of the subsequent
	trip the overall inpact should be close to neutral. The only other alternative is to ignore all data for the vessel which would result in the loss of a large
	amount of data.'''
	column = 'destination_type'
	clause = '''destination_type IN ('P','R','Q')'''

	def summarise(self):
		div = Div()
		count = self.db.Value('''SELECT count(*) FROM checks WHERE code='%s';'''%(self.code()))
		if count==0: div += P('No records were flagged by this check.')
		else:
			div += P('A total of %i records were flagged by this check. The following table summarises the flagged records by destination_type'%count)
			rows = []
			for species in self.dataset.species:
				rows += self.db.Rows('''SELECT 
							species_code,
							destination_type,
							count(*),
							sum(green_weight)/1000,
							sum(green_weight)/(SELECT sum(green_weight) FROM landing WHERE species_code=='%s')*100
						FROM landing 
						WHERE species_code=='%s' AND flags LIKE '%%LADTH%%' 
						GROUP BY destination_type 
						ORDER BY count(*) DESC;'''%(species,species))
			div += FARTable(
				'''Flagged records by species_code and destination_type''',
				('Species','Destination','Records','Landings (t)','Landings (%)'),
				rows
			)
		return div
	
class LADTT(LA):
	drop = True
	brief = 'Landings after transhipping'
	desc = '''Fishers sometimes report having transferred catches to another vessel using destination code 'T'. Unlike for destination codes P,Q and R, these catches can be accounted for
	because the destination vessel is recorded in a column called <i>tranship_vessel_key</i>.  This check retains landings with a destination code 'T' but, to avoid
	double counting, excludes all trips for the destination vessel for the next 3 months. The entire trip is excluded so that species composition is not altered.'''
	column = 'tranship_vessel_key'
	def do(self):
		for row in self.db.Rows('''
			SELECT landing_datetime, tranship_vessel_key,species_code,fishstock_code,green_weight 
			FROM landing WHERE destination_type=='T' AND green_weight>0 AND landing_datetime IS NOT NULL AND tranship_vessel_key IS NOT NULL 
			GROUP BY landing_datetime, tranship_vessel_key
		'''):
			landing_datetime, tranship_vessel_key,species_code,fishstock_code,green_weight  = row
			self.flag(
				clause = '''trip IN (
					SELECT trip FROM landing WHERE vessel_key==%s AND landing_datetime>='%s' AND landing_datetime<date('%s','+3 months')
				)'''%(tranship_vessel_key,landing_datetime,landing_datetime),
				details = species_code
			)
	def summarise(self):
		div = Div()
		count = self.db.Value('''SELECT count(*) FROM (SELECT DISTINCT id FROM checks WHERE code='%s');'''%(self.code()))
		p = P('')
		if count==0: p += 'No records were flagged by this check.'
		else: 
			p += 'A total of %i landing records were flagged by this check.The following'%count
			rows = []
			for species in self.dataset.species:
				rows.append(self.db.Rows('''
				SELECT 
					species_code,
					count(*),
					sum(green_weight)/1000,
					sum(green_weight)/(SELECT sum(green_weight) FROM landing WHERE species_code='%s')*100
				FROM landing
				WHERE flags LIKE '%%LADTT%%' AND species_code=='%s';'''%(species,species)))
			p += FARTable(
				'Summary of records flagged by this check by species.',
				('Species','Landing events','Landings (t)','Landings (%)'),
				rows
			)
			
		p += 'The following table summarises the data for the destination trans-shipment vessel and how much data (if any) was excluded by this check.'
		div += p
		rows = self.db.Rows('''
			SELECT * FROM (
				SELECT tranship_vessel_key AS vessel,count(*) AS ts_events,sum(green_weight )/1000 AS ts_weight
				FROM landing
				WHERE destination_type=='T' AND green_weight>0
				GROUP BY tranship_vessel_key
			) 
			LEFT JOIN (
				SELECT vessel_key AS vessel, count(*) AS fishing_events
				FROM fishing_event
				WHERE vessel_key IN (SELECT DISTINCT tranship_vessel_key FROM landing WHERE destination_type=='T')
				GROUP BY vessel_key
			) USING (vessel)
			LEFT JOIN (
				SELECT vessel_key AS vessel,count(*) AS landing_events,sum(green_weight )/1000 As landing_weight
				FROM landing
				WHERE destination_type=='L' AND vessel_key IN (SELECT DISTINCT tranship_vessel_key FROM landing WHERE destination_type=='T')
				GROUP BY vessel_key
			) USING (vessel)
			LEFT JOIN (
				SELECT vessel_key AS vessel, count(*) AS trips
				FROM (
					SELECT DISTINCT vessel_key,trip FROM landing WHERE flags LIKE '%LADTT%'
				)
				GROUP BY vessel_key
			) USING (vessel)
			LEFT JOIN (
				SELECT vessel_key AS vessel,count(*) AS landing_events,sum(green_weight )/1000 As landing_weight
				FROM landing
				WHERE flags LIKE '%LADTT%'
				GROUP BY vessel_key
			) USING (vessel)
			ORDER BY ts_weight DESC
		''')
		div += FARTable(
			'''Summary of data for trans-shipment destination vessels.''',
			('Vessel','Trans-shipment events','Trans-shipments (t)','Fishing events','Landing events','Landings (t)','Trips excluded','Landing events excluded','Landings excluded (t)'),
			rows
		)
		return div

class LASCF(LA):
	brief = 'State code mistakes'
	desc = '''Starr D.1.4: "Find commonly entered invalid state codes and replace with correct state code"'''
	column = 'state_code'
	def do(self):
		self.change(clause='''state_code IN ('EAT','DIS')''',value='GRE')
		self.change(clause='''state_code='HED' ''',value='HDS')
		self.change(clause='''state_code='TGU' ''',value='HGU')
		self.change(clause='''state_code IN ('GGO','GGT')''',value='GGU')
		
class LASCI(LA):
	brief = 'State code invalid'
	valid = ('GRE','GUT','HGU','DRE','FIL','SKF','USK','SUR','SUR','TSK','TRF','DSC','DVC','MEA','SCT','RLT','TEN','FIN',
			'LIV','MKF','MGU','HGT','HGF','GGU','SHU','ROE','HDS','HET','FIT','SHF','MBS','MBH','MEB','FLP','BEA','LIB',
			'CHK','LUG','SWB','WIN','OIL','TNB','GBP')
	desc = '''
		Change invalid state codes to NULL.
		The following list of valid state_codes was obtained from <a href="http://www.fish.govt.nz/en-nz/Research+Services/Research+Database+Documentation/fish_ce/Appendix+1.htm">the Ministry for Primary Indistries website</a>: %s
	'''%(', '.join(valid))
	column = 'state_code'
	clause = '''state_code NOT IN %s'''%repr(valid)
	value = None
	
class LASCD(LA):
	drop = True
	brief = 'State codes for body parts'
	desc = ''' Starr (2011) suggests "Drop landings where state code==FIN|==FLP|==SHF|==ROE and there is more than one record for the trip/Fishstock combination."'''
	column = 'state_code'
	
	def do(self):
		self.flag(clause='''state_code IN ('FIN','FLP','SCF','ROE')''')
	
	def doNew(self):
		for row in self.db.Rows('''
			SELECT trip,fishstock_code 
			FROM landing
			WHERE trip IN ( SELECT DISTINCT trip FROM landing WHERE state_code IN ('FIN','FLP','SCF','ROE') AND trip IS NOT NULL )
			GROUP BY trip, fishstock_code 
			HAVING count(*)>1;
		'''): self.flag(clause='''trip=%s AND fishstock_code='%s' AND state_code IN ('FIN','FLP','SCF','ROE')'''%row)
			
	def doOld(self):
		##Found this to be old so try the alternative version above
		for row in db.Rows('''SELECT landing.trip,landing.fishstock_code FROM landing INNER JOIN (
				SELECT DISTINCT trip,fishstock_code FROM landing WHERE state_code IN ('FIN','FLP','SCF','ROE') AND trip IS NOT NULL
			) hasCodes ON landing.trip=hasCodes.trip AND landing.fishstock_code=hasCodes.fishstock_code
			GROUP BY landing.trip,landing.fishstock_code HAVING count(*)>1;
		'''): self.flag(clause='''trip=%s AND fishstock_code='%s' AND state_code IN ('FIN','FLP','SCF','ROE')'''%row)
	
class LADUP(LA):
	drop = True
	brief = 'Landing duplicated on CLR form'
	columns = ['vessel_key','landing_datetime','fishstock_code','state_code','destination_type','unit_type','unit_num','unit_weight','green_weight']
	desc = '''Starr (2011) suggests "Look for duplicate landings on multiple (CELR and CLR) forms. Keep only a single version if determined that the records are duplicated"
	If the following fields are duplicated across form types then drop all but the CEL record: %s  Do this after state_code, destination_type etc have been fixed up.'''%(','.join(columns))
	def do(self):
		template = '''form_type!='CEL' '''
		for column in self.columns: template += ''' AND %s='%%s' '''%column
		columnsComma = ','.join(self.columns)
		for row in self.db.Rows('''
			SELECT %s FROM (
				SELECT DISTINCT %s,form_type FROM landing
			) GROUP BY %s HAVING count(*)>1;'''%(columnsComma,columnsComma,columnsComma)): 
			self.flag(clause=template%row)
	def summarise(self):
		div = Check.summarise(self)
		div += FARTable(
			'''%s Errors by fishstock_code,state_code,destination_type'''%self.code(),
			('Fishstock','State','Destination','Records','Landings (t)'),
			self.db.Rows('''SELECT fishstock_code,state_code,destination_type,count(*),sum(green_weight)/1000 FROM landing WHERE flags LIKE '%LADUP%' GROUP BY fishstock_code,state_code,destination_type;''')
		)
		return div
	
class LACFM(LA):
	brief = 'Conversion factor missing'
	desc='''
		Starr suggests "Find missing conversion factor fields and insert correct value for relevant state code and fishing year.
		Missing fields can be inferred from the median of the non-missing fields"
		For each state_code replace missing values with median.
		Starr D.1.3 suggests "Find missing conversion factor fields and insert correct value for relevant state code and fishing year.Missing fields can be inferred from the median of the non-missing fields."
		In this implementation we replace missing values with the median over all fishing_years for that state_code.
	'''
	column = 'conv_factor'
	
	def do(self):
		for species_code,state_code,fishing_year in self.db.Rows('''
			SELECT species_code,state_code,fishing_year 
			FROM landing 
			WHERE state_code IS NOT NULL AND conv_factor IS NOT NULL
			GROUP BY species_code,state_code,fishing_year 
			HAVING count(*)>10000;'''):
			median = self.db.Value('''SELECT median(conv_factor) FROM landing WHERE species_code==? AND state_code=? AND fishing_year==? AND conv_factor IS NOT NULL;''',(species_code,state_code,fishing_year))
			if median is not None: 
				self.change(clause='''conv_factor IS NULL AND species_code=='%s' AND state_code=='%s' AND fishing_year==%s'''%(species_code,state_code,fishing_year),value=median)
	
	def summarise(self):
		div = Check.summarise(self)
		div += FARTable(
			'''%s errors by state_code and replacement conversion factor'''%self.code(),
			('State','Replacement','Records','Landings (t)'),
			self.db.Rows('''SELECT new,count(*),sum(green_weight)/1000 FROM checks LEFT JOIN landing USING(id) WHERE code=='LACFM' GROUP BY new;''')
		)
		return div

class LACFC(LA):
	brief = 'Conversion factor changed'
	desc='''
		Check for any changes in conversion factors and make necessary corrections.
	'''
	column = 'conv_factor'
	
	def do(self):
		##Tabulate the conversion factors used before changing them and so the median for the last year can be back-applied
		self.db.Script('''
			DROP TABLE IF EXISTS check_LACFC;
			CREATE TABLE check_LACFC AS 
			SELECT 
				species_code,
				state_code,
				fishing_year,
				median(conv_factor) AS conv_factor,
				sum(green_weight) AS green_weight
			FROM landing 
			WHERE species_code IS NOT NULL AND state_code IS NOT NULL  AND conv_factor IS NOT NULL
			GROUP BY species_code,state_code,fishing_year;
		''')
		##Only do this for species_code,state_code combinations that have had more than one conversion factor...
		for species_code,state_code in self.db.Rows('''
			SELECT species_code,state_code
			FROM landing
			WHERE species_code IS NOT NULL AND state_code  IS NOT NULL AND conv_factor IS NOT NULL
			GROUP BY species_code,state_code 
			HAVING count(*)>1;'''
		):
			##Get most recent value
			conv_factor = self.db.Value('''SELECT conv_factor FROM check_LACFC WHERE species_code=? AND state_code==? ORDER BY fishing_year DESC LIMIT 1;''',(species_code,state_code))
			##Change conversion factor
			self.change(clause='''species_code=='%s' AND state_code=='%s' AND conv_factor!=%s'''%(species_code,state_code,conv_factor),value=conv_factor)
			##Recalculate green_weight. But only for records where there has been a change in conversion factor
			self.db.Execute('''UPDATE landing SET green_weight=unit_num*unit_weight*conv_factor WHERE species_code=='%s' AND state_code=='%s' AND flags LIKE '%%%s%%' '''%(species_code,state_code,self.code()))

	def summarise(self):
		div = Div()
		##Table of the number of changes made by species, state and old and new
		div += FARTable(
			'Summary of changes to conversion code by species and state. This summary only includes records that were not dropped by other error checks.',
			('Species','State','Original conversion factor','New conversion factor','Records','Landings (t, using new)'),
			self.db.Rows('''
				SELECT species_code,state_code,orig,new,count(*),sum(green_weight)/1000
				FROM checks LEFT JOIN landing USING (id)
				WHERE code=='LACFC' AND dropped IS NULL
				GROUP BY species_code,state_code,orig,new
			''')
		)
		for species in self.dataset.species:
			states = self.db.Values('''SELECT DISTINCT state_code FROM check_LACFC WHERE species_code=='%s';'''%species)
			##Table of median conversion factor by fishing_year and state_code
			medians = self.db.Rows('''SELECT fishing_year,state_code,conv_factor FROM check_LACFC WHERE species_code=='%s';'''%species)
			medians = dict(zip(['%s-%s'%(y,s) for y,s,m in medians],[m for y,s,m in medians]))
			rows = []
			for fy in Check.fishing_years:
				row = [fy]
				for state in states:
					try: median = medians['%s-%s'%(fy,state)]
					except: median = ''
					row.append(median)
				rows.append(row)
			div += FARTable('Median conversion factors for %s in each fishing year by processed state'%species,['Fishing year']+states,rows)
			##Table of sum(green_weight) by fishing_year and state_code
			values = self.db.Rows('''SELECT fishing_year,state_code,round(green_weight/1000,1) FROM check_LACFC WHERE species_code=='%s';'''%species)
			values = dict(zip(['%s-%s'%(y,s) for y,s,v in values],[v for y,s,v in values]))
			rows = []
			for fy in Check.fishing_years:
				row = [fy]
				for state in states:
					try: value = values['%s-%s'%(fy,state)]
					except: value = ''
					row.append(value)
				rows.append(row)
			div += FARTable('Landings (t) of %s in each fishing year by processed state'%species,['Fishing year']+states,rows)
		
		return div
		
class LAGWI(LA):
	brief = 'Green weight imputation'
	desc = '''
		Starr (2011) suggests "Check for missing data in the unit_num and unit_weight fields. Drop records where greenweight=0 or =NULL and either unit_num and unit_weight is missing.
		Missing greenweight can be estimated"
	'''
	column = 'green_weight'
	
	def do(self):
		self.change(clause='''conv_factor IS NOT NULL AND (green_weight=0 OR green_weight IS NULL)''',expr='conv_factor*unit_num*unit_weight',details='1')
		self.change(clause='''conv_factor IS NULL AND (green_weight=0 OR green_weight IS NULL)''',expr='unit_num*unit_weight',details='2')

	def summarise(self):
		return ""
		
class LAGWM(LA):
	drop = True
	brief = 'Green weight missing'
	desc = '''Drop green weights which are zero or NULL.'''
	column = 'green_weight'
	clause = '''(green_weight=0 OR green_weight IS NULL) AND (unit_num IS NULL OR unit_weight IS NULL)'''
		
class LAGWR(LA):
	drop = True
	brief = 'Green weight outliers'
	desc = '''This check follows the method of Starr (2011) for checking for out of range landings.
	'''
	column = 'green_weight'
	
	# Specify which species to run this check on
	species = None

	# The minimum number of landing events
	landings_min = 1000
	
	class Trip:
		'Class for recording details about a trip'
		
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
			
	def do(self):
		##Create table to record details
		self.db.Execute('''DROP TABLE IF EXISTS check_LAGWR''')
		self.db.Execute('''CREATE TABLE IF NOT EXISTS check_LAGWR (species TEXT,method TEXT,events INTEGER,proportion REAL,landings_threshold REAL,cpue_threshold REAL);''')
		
		# Determine the list of species that this check will be conducted on
		if self.species: species_list = self.species
		else:
			species_list = self.db.Values('''
				SELECT species_code
				FROM landing  
				WHERE species_code IS NOT NULL 
				GROUP BY species_code 
				HAVING count(*)>=%s 
				ORDER BY count(*)  DESC;'''%self.landings_min
			)

		##.1 "Find all landing events which are greater than the appropriate ProcA value. Values smaller than ProcA
		## can be used to make a more complete search of the data. Identify the trip numbers associated with these
		## landing events. Calculate for these trips: a) the total greenweight; b) the calculated greenweight"
		print
		for species in species_list: 
			print species
			
			trips = {}
			##.2 "Extract the fishing event data for these trips. Summarise for the trips using method m: a) the total effort; b) the total estimated catch. 
			## Calculate the nominal CPUE (Eq. 1) for each trip t with large landings using method m"
			
			##Need to caculate things for ALL trips because these are used to determine distributions of CPUE etc
				##Paul is using two ratios to further narrow down from this low threshold:
				##	1: sum_green_weight/sum_calc_weight
				##	2: sum_green_weight/sum_catch_weight
				##So need to calculate some of these from landings data
			for row in self.db.Rows('''SELECT trip,sum(green_weight),sum(conv_factor*unit_num*unit_weight) FROM landing WHERE trip IS NOT NULL AND species_code='%s' GROUP BY trip;'''%species): 
				trip = LAGWR.Trip()
				trip.sum_green_weight = row[1]
				trip.sum_calc_weight = row[2]
				if trip.sum_green_weight is not None and trip.sum_calc_weight>0:
					trip.ratio_green_calc = trip.sum_green_weight/trip.sum_calc_weight
				trips[row[0]] = trip
				
			##Caclulate sum of estimated catch for trip
			##If this is a 'numbers' species then multiply by the average weight
			if 0:#!FIXMEself.groom_estimated_subcatch_catch is not None: 
				minimum,average = self.groom_estimated_subcatch_catch[species]	
				adjust = '*%s'%average
			else: adjust = ''
			sql = '''SELECT fishing_event.trip,sum(estimated_subcatch.catch_weight)%s 
			FROM fishing_event,estimated_subcatch 
			WHERE fishing_event.event_key=estimated_subcatch.event_key AND fishing_event.trip IS NOT NULL AND species_code='%s' GROUP BY fishing_event.trip;'''%(adjust,species)
			for row in self.db.Rows(sql):
				try: trip = trips[row[0]]
				except KeyError: continue ##There may be no match because the trip did not land the species but did record it in effort. That does not matter here because were are concerned with landings
				trip.sum_est_weight = row[1]
				if trip.sum_green_weight is not None and trip.sum_est_weight>0:
					trip.ratio_green_est = trip.sum_green_weight/trip.sum_est_weight
				
			##Record this data
			self.db.Execute('''DROP TABLE IF EXISTS check_LAGWR_%s'''%(species))
			self.db.Execute('''
			CREATE TABLE IF NOT EXISTS check_LAGWR_%s (
				trip INTEGER,
				sum_green_weight REAL, 
				sum_calc_weight REAL,
				sum_est_weight REAL, 
				ratio_green_calc REAL, 
				ratio_green_est REAL
			);'''%(species))
			self.db.Cursor.executemany(
				'''INSERT INTO check_LAGWR_%s VALUES(?,?,?,?,?,?)'''%(species),
				[(id, trip.sum_green_weight, trip.sum_calc_weight, trip.sum_est_weight, trip.ratio_green_calc, trip.ratio_green_est) for id,trip in trips.items()])

			##Determine the most important methods for this species in the dataset by finding those that account for 90% of the catch
			overall = self.db.Value('''SELECT count(*) FROM fishing_event WHERE trip IN (SELECT DISTINCT trip FROM landing WHERE species_code=='%s' AND trip IS NOT NULL);'''%species) 
			cumulative = 0.0
			for method,count in self.db.Rows('''
				SELECT primary_method,count(*) 
				FROM fishing_event 
				WHERE trip IN (SELECT DISTINCT trip FROM landing WHERE species_code=='%s'  AND trip IS NOT NULL) 
				GROUP BY primary_method HAVING count(*)>=100
				ORDER BY count(*) DESC;'''%species):
				if cumulative>0.9: break
				proportion = float(count)/overall
				cumulative += proportion
					
				print '  ',method,count,proportion,cumulative
				
				##Clear method related data from trips so it does not 'hangover' from one method to the next
				for trip in trips.values():
					trip.sum_effort = None
					trip.cpue = None
					trip.green_high = 0
					trip.drop = 0
				
				effort_units = {
					'BLL':'total_hook_num',
					'SN':'total_net_length',
					'CP':'max(effort_num,effort_total_num)'
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
					SELECT DISTINCT trip FROM landing WHERE trip IS NOT NULL AND species_code==? AND green_weight>?)''',[method,species,landings_threshold]):
					trip = trips[row[0]]
					trip.green_high = 1
					if (trip.ratio_green_calc>4 or trip.ratio_green_est>4) and trip.cpue>cpue_threshold:
						trip.drop = 1
					elif (trip.ratio_green_calc>10 or trip.ratio_green_est>10) and trip.cpue is None:
						trip.drop = 1
				
				for id,trip in trips.items():
					if trip.drop==1:
						self.flag(
							clause='''trip='%s' AND species_code=='%s' '''%(id,species),
							details = '%s/%s'%(species,method)
						)
				
				self.db.Execute('''DROP TABLE IF EXISTS check_LAGWR_%s_%s'''%(species,method))
				self.db.Execute('''
				CREATE TABLE IF NOT EXISTS check_LAGWR_%s_%s (
					trip INTEGER,
					sum_green_weight REAL, 
					sum_calc_weight REAL,
					sum_est_weight REAL, 
					ratio_green_calc REAL, 
					ratio_green_est REAL, 
					sum_effort REAL, 
					cpue REAL, 
					ok INTEGER, 
					green_high INTEGER, 
					flagged INTEGER
				);'''%(species,method))
				self.db.Cursor.executemany('''INSERT INTO check_LAGWR_%s_%s VALUES(?,?,?,?,?,?,?,?,?,?,?)'''%(species,method),[(id, trip.sum_green_weight, trip.sum_calc_weight, trip.sum_est_weight, trip.ratio_green_calc, trip.ratio_green_est, trip.sum_effort, trip.cpue, trip.ok, trip.green_high, trip.drop) for id,trip in trips.items()])

				##Store results for each species/method
				self.db.Execute('''INSERT INTO check_LAGWR(species,method,events,proportion,landings_threshold,cpue_threshold) VALUES(?,?,?,?,?,?);''',(species,method,count,proportion,landings_threshold,cpue_threshold))
	
	def summarise(self):
		div = Check.summarise(self)
		for index,species in enumerate(self.db.Values('''SELECT DISTINCT species FROM check_LAGWR;''')):
			div += H3('%s'%species)
			
			##Summarise the estimated & landing info
			row = self.db.Rows('''SELECT trip, sum_green_weight, sum_calc_weight, sum_est_weight, ratio_green_calc, ratio_green_est FROM check_LAGWR_%s;'''%(species))
			##Histograms of catch/landings ratios
			for field,label in [
				('ratio_green_calc','Landed green weight/Landed calculated weight'),
				('ratio_green_est','Landed green weight/Estimated catch'),
			]: div += self.histogram(
				'check_LAGWR_%s'%(species),
				field,
				xlab=label,
				lines=(0.75,1.33,4),
				caption="Frequency distribution of %s for %s"%(label.lower(),species)
			)
			
			for method,landings_threshold,cpue_threshold in self.db.Rows('''SELECT method,landings_threshold,cpue_threshold FROM check_LAGWR WHERE species=='%s';'''%species):
				div += H4('%s/%s'%(species,method))
				div += P('Landings threshold: %.2f<br>CPUE threshold: %.2f'%(landings_threshold,cpue_threshold))
				
				table = 'check_LAGWR_%s_%s'%(species,method)
				div += self.histogram(table,'sum_green_weight',where='sum_effort>0',lines=[landings_threshold],transform='log10',xlab='Landings',ylab='Trips',
					caption='Landings for trips that used %s and landed %s.'%(method,species))
				div += self.histogram(table,'cpue',where='ok=1',lines=[cpue_threshold],transform='log10',xlab='CPUE',ylab='Trips',
					caption='CPUE for trips that used %s and landed %s and which had the ratio of green_weight to estimated_weight between 0.75 and 1.33.'%(method,species))
				
				##Summarise those trips that were flagged
				flagged = self.db.Rows('''SELECT * FROM %s WHERE flagged==1;'''%table)
				if len(flagged)>0: div += FARTable('Details of trips flagged',[col[0] for col in self.db.Cursor.description],flagged)
				else: div += 'No trips dropped by this check.'
		return div