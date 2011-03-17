from check import *

class TDSTA(CHSTA):
	table = 'trip_details'
	
	def do(self):
		CHSTA.do(self)
		self.db.Alter('''ALTER TABLE trip_details ADD COLUMN fishing_year INTEGER;''') 
		self.db.Execute('''UPDATE trip_details SET fishing_year=strftime('%Y',end_datetime);''')
		self.db.Execute('''UPDATE trip_details SET fishing_year=fishing_year+1 WHERE strftime('%m',end_datetime)>="10";''')
		self.db.Execute('''CREATE INDEX IF NOT EXISTS trip_details_fishing_year ON trip_details(fishing_year);''')
	
class TDDAB(Check):
	table = 'trip_details'
	brief = 'Calculate the best date for a trip'
	desc = '''Starr D.1.8 describes a method for calculating the "best date" for a trip.  This involves determining the length (in days) of each trip,  
		calculating the 95th percentile of trip length for each type of form and using that as the basis for determining if the trip end date is likely.  The field <i>best_date</i> which is created is not actually used for characterization and CPUE analyses 
		because the field <i>fishing_event.start_datetime</i> is used instead.  However, the following summaries may indicated potential problems with the asignment of a trip number'''
	
	def do(self):
		##Start by adding landing_datetime to make calcs easier
		self.db.Alter('''ALTER TABLE trip_details ADD COLUMN landing_datetime DATETIME;''')
		self.db.Execute('''UPDATE trip_details SET landing_datetime=(SELECT landing.landing_datetime FROM trip_details LEFT JOIN landing ON trip_details.trip=landing.trip)''')
		##.1
		self.db.Alter('''ALTER TABLE trip_details ADD COLUMN trip_length INTEGER;''')
		self.db.Execute('''UPDATE trip_details SET trip_length=julianday(end_datetime)-julianday(start_datetime);''')
		self.db.Alter('''ALTER TABLE trip_details ADD COLUMN trip_length_alt INTEGER;''')
		self.db.Execute('''UPDATE trip_details SET trip_length_alt=julianday(end_datetime)-julianday(landing_datetime) WHERE landing_datetime<end_datetime;''')
		##Produce summary table
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
		lastValidDate = datetime.datetime.now().strftime('%Y-%m-%d')
		self.db.Execute('''UPDATE trip_details SET best_date=? WHERE landing_datetime>?;''',(lastValidDate,lastValidDate))
		##'Round' off to a date
		self.db.Execute('''UPDATE trip_details SET best_date=strftime('%Y-%m-%d',best_date);''')
		##Reset fishing year usgin the best date
		self.db.Execute('''UPDATE trip_details SET fishing_year=strftime('%Y',best_date);''')
		self.db.Execute('''UPDATE trip_details SET fishing_year=fishing_year+1 WHERE strftime('%m',best_date)>="10";''')
		
	def summarise(self):
		return self.histogram('trip_details','trip_length',xlab='Trip length (days)',ylab='Trips',caption='Frequency distibution of trip length for all trips in dataset.')

if __name__=='__main__':
	Check.db = Database('/Trophia/Tanga/Data/spo18_mfish_spo201001/database.db3')
	for check in [
		TDSTA,
		TDDAB
	]:
		inst = check()
		inst.do()
		inst.view()