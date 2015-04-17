from check import *

class FESTA(CHSTA):
	table = 'fishing_event'
	
	def do(self):
		CHSTA.do(self)
		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN date DATETIME;''')  
		self.db.Execute('''UPDATE fishing_event SET date=strftime('%Y-%m-%d',start_datetime);''')
		self.db.Execute('''CREATE INDEX IF NOT EXISTS fishing_event_date ON fishing_event(date);''')
		
		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN fishing_year INTEGER;''') 
		self.db.Execute('''UPDATE fishing_event SET fishing_year=strftime('%Y',date);''')
		self.db.Execute('''UPDATE fishing_event SET fishing_year=fishing_year+1 WHERE strftime('%m',date)>="10";''')
		self.db.Execute('''CREATE INDEX IF NOT EXISTS fishing_event_fishing_year ON fishing_event(fishing_year);''')
		
		##For TCP forms the effort_num is always 1
		self.db.Execute('''UPDATE fishing_event SET effort_num=1 WHERE form_type='TCP';''')
	
class FE(Check):
	table = 'fishing_event'
		
class FESDM(FE):
	brief = 'Start date/time is missing'
	desc = '''
		The starting date/time for a fishing event can be missing. This check flags those records but no attempt is made to impute the date/time.
	'''
	column = 'start_datetime'
	clause = '''start_datetime IS NULL'''
	
class FESDF(FE):
	brief = 'End date/time is in the future'
	desc = '''
		The starting date/time for a fishing event can be in the future. This check flags those records but no attempt is made to correct date/time.
	'''
	column = 'start_datetime'
	clause = '''start_datetime>datetime('now')'''
	
class FEPMI(FE):
	brief = 'Method imputation'
	desc = '''		
		This check follows the suggestion of Starr (2011) to replace missing values for the column <i>primary_method</i> with the value recorded for other fishing events in the trip providing those other values are all the same.
	'''
	column = 'primary_method'
	
	def do(self):
		##Find all records with missing primary_method. For each trip in this set count number of primary_method used. If only one, then replace otherwise drop the trip
		for row in self.db.Rows('''SELECT trip,primary_method FROM (
			SELECT DISTINCT trip,primary_method FROM fishing_event WHERE trip IN (SELECT DISTINCT trip FROM fishing_event WHERE primary_method IS NULL AND trip IS NOT NULL) AND primary_method IS NOT NULL
		) GROUP BY trip HAVING count(*)==1'''): 
			self.change(clause='''primary_method IS NULL AND trip=%s'''%row[0],value=row[1])
			
	def summarise(self):
		div = Div()
		div += FARTable(
			'''Number of fishing events by imputed value for <i>primary_method</i>.''',
			('Method','Fishing events'),
			self.db.Rows('''
				SELECT primary_method, count(*) 
				FROM fishing_event
				WHERE flags LIKE '%%%s%%'
				GROUP BY primary_method
			'''%self.code())
		)
		return div
			
class FEPMM(FE):
	brief = 'Method missing'
	desc = '''		
		This check follows the suggestion of Starr (2011) to flag entire trips which have an event with a missing method.
	'''
	column = 'primary_method'
	clause = ''' trip IN (SELECT DISTINCT trip FROM fishing_event WHERE primary_method IS NULL AND trip IS NOT NULL)'''

class FETSE(FE):
	brief = 'Target species error'
	desc = '''		
		Replace invalid target species codes with the correct code where this is able to be inferred.
	'''
	column = 'target_species'
	
	def do(self):
		self.change(clause=''' target_species=='BAS' ''',value='HPB')
		self.change(clause=''' target_species=='HAP' ''',value='HPB')

	def summarise(self):
		div = Div()
		div += FARTable(
			'''Number of fishing events where an invalid <i>target_species</i> code was replaced. A maximum of the top ten invalid codes are listed.''',
			('Orginal value','Replacement value','Fishing events'),
			self.db.Rows('''
				SELECT orig,new,count(*) 
				FROM checks 
				WHERE code=='FETSE' 
				GROUP BY orig, new
				ORDER BY count(*) DESC 
				LIMIT 10;
			''')
		)
		return div
	
class FETSW(FE):
	brief = 'Target species invalid'
	desc = '''		
		Where the target species code is not amongst the list of valid species codes it is flagged and set to NULL so that it may be subsequently corrected during imputation.
	'''
	column = 'target_species'
	clause = '''target_species NOT IN (
		'AGR','ALB','ANC','ANG','ATO','BAR','BBE','BCA','BCD','BCO','BCR','BEA','BEE','BEL','BEM','BFL','BGZ','BIG','BKM','BMA','BNS','BOA','BOE','BOX','BPF','BRA','BRC','BRI','BRZ','BSH','BSK','BSP','BSQ','BTU','BUT',
		'BWH','BWS','BYA','BYX','CAC','CAN','CAR','CDL','CHC','CHI','CMO','COC','COE','COL','CON','CRA','CRB','CSQ','CTU','CYO','CYP','DAN','DEA','DIS','DOF','DSK','DSS','DSU','DWD','DWE','ECO','EEL','EGR','ELE',
		'EMA','EMP','EPD','EPL','EPR','ERA','ESO','ETB','ETL','FHD','FLA','FLY','FRO','GAR','GFL','GLM','GMU','GRA','GSC','GSE','GSH','GSP','GSQ','GTR','GUR','HAP','HAG','HAK','HEP','HHS','HJO','HOK','HOR','HPB','ICX',
		'JAV','JDO','JGU','JMA','KAH','KBB','KBL','KEL','KIC','KIN','KOH','KTA','KWH','LAN','LCH','LDO','LEA','LEG','LEP','LES','LFB','LFE','LIM','LIN','LSO','MAK','MCA','MDI','MDO','MMI','MOK','MOO','MOR','MRL','MSG','MSP','MUN','MUS',
		'NOT','NSD','NTU','OAR','OCT','OEO','OFH','ONG','OPE','ORH','OSD','OYS','OYU','PAD','PAR','PAU','PDG','PDO','PGR','PHC','PIG','PIL','PIP','PMA','POP','POR','POS','POY','PPI','PRA','PRK','PTE','PTO','PZL','QSC',
		'RAG','RAT','RBM','RBT','RBY','RCO','RDO','RHY','RIB','RMO','ROC','RPE','RRC','RSK','RSN','RUD','SAE','SAI','SAL','SAM','SAU','SBK','SBO','SBW','SCA','SCC','SCG','SCH','SCI','SCM','SCO','SDO','SEM','SEV','SFE',
		'SFI','SFL','SFN','SKA','SKI','SKJ','SLG','SLK','SLO','SMC','SNA','SND','SOR','SPD','SPE','SPF','SPI','SPO','SPP','SPR','SPZ','SQU','SQX','SRR','SSF','SSH','SSI','SSK','SSO','STA','STM','STN','STR','STU','SUN','SUR',
		'SWA','SWO','TAR','THR','TOA','TOR','TRE','TRS','TRU','TUA','TUR','VCO','WAH','WAR','WGR','WHE','WHR','WHX','WIT','WOE','WRA','WSE','WSQ','WWA','YBF','YEM','YFN'
	)'''
	value = None

	def summarise(self):
		div = Div()
		div += FARTable(
			'''Number of fishing events where an invalid <i>target_species</i> code was made NULL. A maximum of the top ten invalid codes are listed.''',
			('Target species','Fishing events'),
			self.db.Rows('''
				SELECT orig,count(*) 
				FROM checks 
				WHERE code=='FETSW' 
				GROUP BY orig 
				ORDER BY count(*) DESC 
				LIMIT 10;
			''')
		)
		return div
	
class FETSI(FE):
	brief = 'Target species imputation'
	desc = '''		
		This check follows the suggestion of Starr (2011) to replace missing values for the column <i>target_species</i> with the most frequently recorded value for other fishing events in the trip.
	'''
	column = 'target_species'
	
	def do(self):
		for trip in self.db.Values('''SELECT DISTINCT trip FROM fishing_event WHERE target_species IS NULL AND trip IS NOT NULL'''):
			value = self.db.Value('''SELECT target_species FROM fishing_event WHERE target_species IS NOT NULL AND trip=? GROUP BY target_species ORDER BY count(*) DESC LIMIT 1''',[trip])
			if value: self.change(clause='''target_species IS NULL AND trip=%s'''%trip,value=value)
			
	def summarise(self):
		div = Div()
		div += FARTable(
			'''Number of fishing events by imputed value for <i>target_species</i>. A maximum of the top ten imputed codes are listed.''',
			('Target species','Fishing events'),
			self.db.Rows('''
				SELECT target_species, count(*) 
				FROM fishing_event
				WHERE flags LIKE '%%%s%%'
				GROUP BY target_species
				ORDER BY count(*) DESC
				LIMIT 10
			'''%self.code())
		)
		return div
		
class FETSM(FE):
	brief = 'Target species missing'
	desc = '''		
		This check follows the suggestion of Starr (2011) to flag entire trips which have an event with a missing target species.
	'''
	column = 'target_species'
	clause = '''trip IN (SELECT DISTINCT trip FROM fishing_event WHERE target_species IS NULL AND trip IS NOT NULL)'''

class FESAS(FE):
	brief = 'Statistical area set incorrect'
	desc = '''		
		There are several sets of statistical area (e.g. general, rock lobster, scallop). The wrong set can used be used. In particular,
		fishers that often use rock lobster stat areas can erroneously use them when using methods for which the general statistical areas
		should be applied. Currently, this check changes rock lobster stat areas to general stat areas where there is an exact or
		close match in the areas. Currently, this is only implemented for cod potting in the Chatham Islands.
	'''
	column = 'start_stats_area_code'
	
	def do(self):
		for cra,gen in (
			('940','049'),
			('941','050'),
			('942','051'),
			('943','052'),
		):
			self.change(
				clause = '''primary_method=='CP' AND start_stats_area_code==%s'''%cra,
				value = gen
			)

	def summarise(self):
		div = Div()
		div += FARTable(
			'''Numbers of fishing events for which statistical area was changed by the FESAS check by method.''',
			('Method','Original','New','Fishing events'),
			self.db.Rows('''
				SELECT primary_method,orig,new,count(*)
				FROM checks LEFT JOIN fishing_event USING (id)
				WHERE checks.code=='FESAS'
				GROUP BY primary_method,orig,new
			''')
		)
		return div
	
class FESAI(FE):
	brief = 'Statistical area imputation'
	desc = '''		
		Starr (2011) suggests to search for missing statistical area fields and substitute the 'predominant' (most frequent) statistical area for the trip for trips which report the statistical area fished in other records.
	'''
	column = 'start_stats_area_code'
	
	def do(self):
		for trip in self.db.Values('''SELECT DISTINCT trip FROM fishing_event WHERE start_stats_area_code IS NULL AND trip IS NOT NULL'''):
			value = self.db.Value('''SELECT start_stats_area_code FROM fishing_event WHERE start_stats_area_code IS NOT NULL AND trip=? GROUP BY start_stats_area_code ORDER BY count(*) DESC LIMIT 1''',[trip])
			if value: self.change(clause='''start_stats_area_code IS NULL AND trip=%s'''%trip,value=value)

	def summarise(self):
		return ""
		
class FESAM(FE):
	brief = 'Statistical area missing'
	desc = '''
		This check follows the suggestion of Starr (2011) to flag entire trips which have an event with a missing statistical area
	'''
	column = 'start_stats_area_code'
	clause = '''trip IN (SELECT DISTINCT trip FROM fishing_event WHERE start_stats_area_code IS NULL AND trip IS NOT NULL)'''
	
class FELLI(FE):
	brief = 'Position (lat/lon) imputation'
	desc = '''
		This checks sets the fields <i>start_latitude,start_longitude,end_latitude,end_longitude</i> to NULL where they are 999.9 (the MPI value indicating null latitude or longitude).
		It also creates the fields <i>lat</i> and <i>lon</i> which are equal to the <i>start_latitude</i> and <i>start_longitude</i> unless those are missing in which
		case end positions are used.
	'''
	column = 'lat/lon'

	def do(self):
		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN lat REAL;''') 
		self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN lon REAL;''')  
		
		##Set lat and lon to NULL where 999.9
		for field in ('start_latitude','start_longitude','end_latitude','end_longitude'):
			self.db.Execute('''UPDATE fishing_event SET %s==NULL WHERE %s=999.9'''%(field,field))
			
		self.db.Alter('''UPDATE fishing_event SET lat=start_latitude, lon=start_longitude;''')  
		self.db.Alter('''UPDATE fishing_event SET lat=end_latitude WHERE lat IS NULL;''') 
		self.db.Alter('''UPDATE fishing_event SET lon=end_longitude WHERE lon IS NULL;''') 

	def summarise(self):
		return ''
		
class FELLS(FE):
	brief = 'Position (lat/lon) outside of statistical area'
	desc = '''
		Where the lat/lon position is outside of the bounding box of the statistical area then set lat/lon to NULL
	'''
	column = 'lat/lon'
	
	def do(self):
		##Change lats and lons.
		for stat,latmin,latmax,lonmin,lonmax in self.db.Rows('''SELECT * FROM stats_boxes;'''):
			self.change(column='lat',clause='''start_stats_area_code=='%s' AND lat NOT BETWEEN %s-0.1 AND %s+0.1'''%(stat,latmin,latmax),value=None)
			self.change(column='lon',clause='''start_stats_area_code=='%s' AND lon NOT BETWEEN %s-0.1 AND %s+0.1'''%(stat,lonmin,lonmax),value=None)

	def summarise(self):
		return ""
		
			
class FEFMA(FE):
	brief = 'Fisheries management area ambiguous'
	desc = '''Starr (2011) suggest to mark trips which landed to more than one fishstock for straddling statistical areas. Since this relies on fishing event.start_stats_area_code do this check after grooming on that.'''
	
	def do(self):
		##For each species determine those events which may be outside the area of interest...
		for species,fishstocks_interest in self.dataset.fishstocks.items(): 
			##..for each stat that belongs to 2 or more FMAs for this species...
			for stat in self.db.Values('''
				SELECT DISTINCT start_stats_area_code 
				FROM fishing_event 
				WHERE start_stats_area_code IN (SELECT stat FROM qmastats WHERE species=='%s' GROUP BY stat HAVING count(*)>=2);'''%species):
				##...get the fishstocks that could be landed to for this stat,
				fishstocks_stat = tuple(self.db.Values('''SELECT qma FROM qmastats WHERE species='%s' AND stat='%s';'''%(species,stat)))
				##...for each trip that fished in this stat...
				for trip in self.db.Values('''SELECT DISTINCT trip FROM fishing_event WHERE start_stats_area_code='%s' AND (lat IS NULL OR lon IS NULL) AND trip IS NOT NULL'''%stat):
					fishstocks_landed = self.db.Values('''SELECT DISTINCT fishstock_code FROM landing WHERE trip=%s AND fishstock_code IN %s;'''%(trip,repr(fishstocks_stat)))
					##...see if there were landings to more than one of these fishstocks
					if len(fishstocks_landed)>1: 
						self.flag(clause='''trip=%s AND start_stats_area_code='%s' AND (lat IS NULL OR lon IS NULL)'''%(trip,stat),details=','.join(fishstocks_landed))
						
	
	def summarise(self):
		div = Div()
		div += FARTable(
			'''Numbers of fishing events that fished in straddling statistical areas and that landed to more than one Fishstock that occurs in that statistical area.''',
			('Statistical area','Fishing events flagged','Fishing events flagged (%)'),
			self.db.Rows('''
			SELECT start_stats_area_code, flagged, flagged/(total*1.0)*100 AS percent FROM (
				SELECT start_stats_area_code, count(*) AS flagged FROM fishing_event WHERE flags LIKE '%%%s%%' GROUP BY start_stats_area_code
			) INNER JOIN (
				SELECT start_stats_area_code, count(*) AS total FROM fishing_event GROUP BY start_stats_area_code
			) USING (start_stats_area_code) 
			GROUP BY start_stats_area_code
			HAVING start_stats_area_code in (%s); '''%(self.code(),','.join([repr(stat) for stat in self.dataset.statareas])))
		)
		div += FARTable(
			'''Numbers of fishing events that fished in straddling statistical areas and that landed to more than one Fishstock that occurs in that statistical area.''',
			('Statistical area','Fishstocks in landings','Fishing events'),
			self.db.Rows('''
			SELECT start_stats_area_code, details,count(*) 
			FROM checks LEFT JOIN fishing_event USING (id) 
			WHERE code=='%s' AND start_stats_area_code IN (%s)
			GROUP BY start_stats_area_code,details'''%(self.code(),','.join([repr(stat) for stat in self.dataset.statareas])))
		)
		return div
		
class FEETN(FE):
	brief = 'Consistency between effort number fields'
	desc = '''
		For the methods RLP, CP, EP, FP and FN, the field <i>effort_total_num</i> is used for the "Number of pot/trap lifts in the day" and <i>effort_num</i> is used for
		the "Number of pots/traps in the water at midgnight". For the CP method, these fields are checked for consistency. Records are flagged where the number of pots lifted was more than 9 times the 
		number left overnight and the number of pots lifted was more than 100. Where <i>effort_total_num</i> is missing and  <i>effort_num</i> is greater than zero <i>effort_total_num</i> is replaced with <i>effort_num</i>.
	'''
	column = 'effort_total_num'
	
	def do(self):
		self.flag(clause = '''primary_method=='CP' AND effort_total_num>effort_num*9 AND effort_total_num>100 AND effort_num>10''')
		self.change(clause = '''primary_method=='CP' AND effort_total_num IS NULL AND effort_num>0''',expr='''effort_num''')

	def summarise(self):
		div = Div()
		div += P('''The following figures provide summaries of the relationship between the total potlifts and the number of pots in the water at midnight for 
			events where cod potting (CP) was the primary method.''')
		div += FARTable(
			'''The number of fishing events flagged according by effort_total_num (using bins of width 10)''',
			('effort_total_num','Fishing events flagged'),
			self.db.Rows('''SELECT CAST(effort_total_num/10 AS INTEGER)*10 AS effort_total_num, count(*) AS flagged FROM fishing_event WHERE flags LIKE '%%%s%%' GROUP BY CAST(effort_total_num/10 AS INTEGER)*10; '''%self.code())
		)
		for method in ('CP',):
			div += self.histogram(
				'fishing_event','effort_total_num/effort_num',
				xlab='effort_total_num/effort_num',
				ylab='Events',
				where="primary_method=='%s'"%method,
				caption = '''Histogram of the ratio of total potlifts (effort_total_num ) over pots in the water at midnight (effort_num) for method %s.'''%method
			)
			div += self.scatterplot(
				'fishing_event','effort_num','effort_total_num',
				transform='log10',
				xlab='effort_num',
				ylab='effort_total_num',
				where="primary_method=='%s'"%method,
				lines=[(0,1),(0,9)],
				caption = '''Relationship between total potlifts (effort_total_num) and pots in the water at midnight (effort_num) for method %s.'''%method
			)
		return div

class FEEHN(FE):
	brief = 'Transposing of number of sets and and total hook number for lining methods on CELR forms'
	desc = '''
		On CELR forms, events which use a lining methods (BLL,SLL,DL and TL) are mean to record the "Number of sets hauled in a day " into the <i>effort_num</i> field
		and the "Total number of hooks hauled in the day" in the <i>total_hook_num</i> field. These can be transposed. In this check for CELR forms with these methods, where <i>effort_num</i> is geater or
		equal to 100 and <i>total_hook_num</i> is less than 100, these fields are transposed.

	'''
	column = 'effort_num/total_hook_num'
	
	def do(self):
		for id,effort_num,total_hook_num in self.db.Rows('''SELECT id,effort_num,total_hook_num FROM fishing_event WHERE form_type=='CEL' AND primary_method IN ('BLL','SLL','DL','TL') AND effort_num>=100 AND total_hook_num<100'''):
			self.db.Execute('''UPDATE fishing_event SET effort_num=%s, total_hook_num=%s, flags=flags||'%s ' WHERE id=%s'''%(total_hook_num,effort_num,'FEEHN',id))
			self.db.Execute('''INSERT INTO checks(code,"table",id) VALUES ('%s','fishing_event',%s)'''%('FEEHN',id))
		
class FEEMU(FE):
	brief = 'Incorrect measurement units for effort fields'
	desc = '''
		For netting methods, the mesh size (field `effort_width`) can be recorded in inches (e.g. 4) instead of mm (i.e. 100).
		This check replaces all effort_width less than or equal to ten with the value time 25 (e.g. 4 becomes 100)
	'''
	column = 'effort_width'
	criteria = 'primary_method=="SN" AND effort_width<=10'
	value = 'effort_width*25'

class FEEFO(FE):
	brief = 'Outliers for effort fields'
	desc = '''
		This check exammines effort fields by method and form type to detect and correct outlier values.
		For each form type, checks were done on the most important effort fields. 
	'''
	column = 'various'
	##The minimum number of events that a primary_method/form_type must have to be considered
	events = 10000
	##The percentage of events where the field is not null that a primary_method/form_type must have to be considered
	notnulls = 80

	def do(self):
		self.db.Script('''
		DROP TABLE IF EXISTS check_FEEFO;
		CREATE TABLE check_FEEFO (
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
			ceil REAL,
			multiplier REAL,
			lower REAL,
			upper REAL,
			substitutions REAL);''')
		
		print
		for field in [
			'effort_num',
			'effort_total_num',
			'fishing_duration',
			'total_hook_num',
			'total_net_length',
		]:
			##"1. For a given effort field, select a partial dataset from the fishing event data based on a method of capture and a form type (CELR or TCEPR)"
			##For each form_type & primary method combination which has at least 1000 not NULL records and >=80% of records not NULL
			for form_type,primary_method,count in self.db.Rows('''
				SELECT form_type,primary_method,count(*) 
				FROM fishing_event 
				WHERE form_type IS NOT NULL AND primary_method IS NOT NULL AND %s IS NOT NULL 
				GROUP BY form_type,primary_method
				ORDER BY count(*) DESC
				'''%(field)):
				total = self.db.Value('''SELECT count(*) FROM fishing_event WHERE primary_method=? AND form_type=?''',(primary_method,form_type))
				percent = float(count)/total*100 if total>0 else 0
				
				print field,form_type,primary_method,count,'%s%%'%round(percent,1),; sys.stdout.flush()
				if count>=self.events and percent>=self.notnulls:
					
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
					floor = 0.0
					ceil = 1e9
					if field=='effort_num': 
						floor = 1
					elif field=='fishing_duration':
						floor = round(5/60.0,3) ##5 minutes
						ceil= 24.0
						if primary_method=='MW': 
							floor = round(5/60.0,3) ##5 minutes
						elif primary_method=='BT':
							floor = round(10/60.0,3) ## 10 minutes
						elif primary_method=='SN':
							floor = 2.0 ## 2 hours
							ceil = 24.0 * 5 ## 5 days
					print p10,fleet_median,p90,floor,ceil,; sys.stdout.flush()
					
					multiplier = 2.0
					substitution_thresh = 1
					while True:
						print '>',; sys.stdout.flush()
						
						lower,upper = p10/multiplier,p90*multiplier
						if lower<floor: lower = floor
						if upper>ceil: upper = ceil
						
						##"6. Compare the effort value in every record with the trial bounds determined in Step 1. Substitute the vessel
						##median if the effort value lies outside of the bounds."
						##At this stage I just count the number of substitutions that would be made given lower and upper
						##Actual substitutions are not done until the right multiplier has been determined based on substiution proportion
						##This avoids calculating vessel median for every trial value of multiplier
						substitutions = self.db.Value('''SELECT count(*) FROM fishing_event WHERE primary_method=? AND form_type=? AND (%s<? OR %s>?)'''%(field,field),(primary_method,form_type,lower,upper))/float(n)*100
						
						##"7. Repeat this procedure with alternative bounds if the number of substitutions is larger than 1% (changed from 5% to be more conservative in changing data) of the total
						##number of records."
						if substitutions<=substitution_thresh: break
						else:
							if (lower==floor and upper==ceil) or multiplier>=99.99: break
							else: multiplier += 0.1
					print multiplier,'%s%%'%round(substitutions,3),; sys.stdout.flush()
					
					##"4. Calculate the median value of the effort field for every vessel in the selected dataset. Substitute the median
					##value calculated in Step 1 if the median value for the vessel lies outside of the calculated bounds."
					##Find vessels that have any values for field outside of bounds
					##Pre-calculate vessel medians (this is faster than doing it separately for each vessel)
					vessel_medians = self.db.Rows('''
						SELECT vessel_key,median(%s) 
						FROM fishing_event 
						WHERE vessel_key IN (
							SELECT DISTINCT vessel_key 
							FROM fishing_event 
							WHERE vessel_key IS NOT NULL AND primary_method=? AND form_type=? AND (%s IS NULL OR (%s<? OR %s>?))
						) AND primary_method=? AND form_type=? AND %s IS NOT NULL
						GROUP BY vessel_key'''%(field,field,field,field,field),(primary_method,form_type,lower,upper,primary_method,form_type))
					vessel_medians = dict(vessel_medians)
					##"5. Create a temporary field on every record in the partial dataset which contains the median value of the target
					##effort field for the vessel appropriate to the record. This field will contain the fleet median for vessels which
					##were outside of the bounds as defined in Step 3."
					for vessel,vessel_median in vessel_medians.items():
						##Calculate vessel median
						median_type = 'VM'
						value = vessel_median
						##Replace value with fleet_median if outside of bounds or never recorded any effort of this method
						if value is None or value<lower or value>upper:
							median_type = 'FM'
							value = fleet_median
						##Do replacement 
						self.change(
							column = field,
							clause = '''vessel_key=%s AND primary_method='%s' AND form_type='%s' AND (%s IS NULL OR (%s<%s OR %s>%s))'''%(vessel,primary_method,form_type,field,field,lower,field,upper),
							value = value,
							details = median_type
						)
				
					##Record
					self.db.Execute('''INSERT INTO check_FEEFO VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);''',[field,form_type,primary_method,count,percent,mi,p10,fleet_median,p90,ma,floor,ceil,multiplier,lower,upper,substitutions])
				else:
					self.db.Execute('''INSERT INTO check_FEEFO(field,form_type,primary_method,count,percent) VALUES(?,?,?,?,?);''',[field,form_type,primary_method,count,percent])
					
				print 
				sys.stdout.flush()
				
	def summarise(self):
		div = Div()
		div += P('''
			Checks were only done on a form type/field combination is there were at least %s records 
			and that the number of NULL values for that field for that form type were less than %s%% of all records. These criteria avoid running checks on data that form a small part of the dataset
			or which are recorded on an optional basis. The following table lists the form type/effort field combinations for which this check was done.
		'''%(self.events,100-self.notnulls))
		
		div += FARTable(
			'''Results from the the FEEFO check on effort fields. The minimum, 10th percentile, median, 90th percentile and maximum are calculated prior to anges changes to data applied by this check. The
			Floor and cieling are the minimum and maximum values that are applied to each effort field. The multiplier is the final multiplier used to convert the 10th and 90th percentiles into the lower and upper values
			used to define if a substitution is done.''',
			('Effort field','Form type','Method','Records not null','Records not null (%)','Minimum','10th percentile','Median','90th percentile','Maximum','Floor','Ceiling','Multiplier','Lower','Upper','Substitutions (%)'),
			self.db.Rows('''SELECT * FROM check_FEEFO WHERE substitutions IS NOT NULL;'''))
			
		rows = self.db.Rows('''
			SELECT primary_method,form_type,column,fishing_year,count(*) AS count
			FROM checks LEFT JOIN fishing_event USING (id)
			WHERE code=='FEEFO' AND primary_method IS NOT NULL AND form_type IS NOT NULL AND column IS NOT NULL AND fishing_year IS NOT NULL
			GROUP BY primary_method,form_type,column,fishing_year
		''')
		rows = robjects.DataFrame({
			'group': robjects.FactorVector(['%s-%s-%s'%(row[0],row[1],row[2]) for row in rows]),
			'fishing_year': robjects.IntVector([row[3] for row in rows]),
			'count': robjects.IntVector([row[4] for row in rows])
		})
		#plot = ggplot2.ggplot(rows) + ggplot2.aes_string(x='fishing_year',y='count',colour='group') + ggplot2.geom_point() 
		#plot.plot()
			
		for method,column,form in self.db.Rows('''SELECT primary_method,field,form_type FROM check_FEEFO WHERE substitutions IS NOT NULL;'''):
			rows = self.db.Rows('''
				SELECT orig,new,count(*) AS count
				FROM checks LEFT JOIN fishing_event USING (id)
				WHERE code=='FEEFO' AND primary_method=='%s' AND column=='%s' AND form_type=='%s' 
				GROUP BY orig,new
			'''%(method,column,form))
			
			if len(rows)>0:
				pass
				
				#filename = 'summary/FEEFO Orig New %s%s%s.png'%(method,column,form)
				#R.png(filename,600,400)
				#R.plot([x for x,y,c in rows],[y for x,y,c in rows],cex=[math.sqrt(c) for x,y,c in rows],xlab='Original value',ylab='Substituted value',pch=1)
				#plot = ggplot2.ggplot(rows) + ggplot2.aes_string(x='fishing_year',y='count',colour='group') + ggplot2.geom_point() 
				#plot.plot()
				#R.dev_off()
				#div += FARFigure(filename,'%s %s %s'%(method,column,form))
				
				#filename = 'summary/FEEFO Hist Old %s%s%s.png'%(method,column,form)
				#R.png(filename,600,400)
				#R.hist([float(x) for x,y,c in rows if x is not None],xlab='Original value',main='',breaks=30)
				#R.dev_off()
				#div += FARFigure(filename,'%s %s %s'%(method,column,form))
			
		return div

