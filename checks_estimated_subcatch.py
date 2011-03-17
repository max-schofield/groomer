from check import *

class ESSTA(CHSTA):
	table = 'estimated_subcatch'
	
	def do(self):
		CHSTA.do(self)
			
class ESCWN(Check):
	table = 'estimated_subcatch'
	brief = 'Estimated catch is recorded as numbers not weight'
	desc = '''Should only be run for species that use count for catch_weight (tuna species and others?)'''
	def do(self):
		self.db.Execute('''DROP TABLE IF EXISTS ESCWN''')
		self.db.Execute('''CREATE TABLE ESCWN (species TEXT,records INTEGER);''')
		##For each species in the estimated_subcatch table
		for species,count in self.db.Rows('''SELECT species_code,count(*) FROM estimated_subcatch WHERE species_code IN ('ALB') GROUP BY species_code ORDER BY count(*)  DESC;'''): 
			self.db.Execute('''INSERT INTO check_ESCWN VALUES(?,?);''',(species,count))
			##Compare each trips estimated catch to landed green weight to see if it lies below minimum.
			##If it does then it is assumed to represent a weight and so is converted using the average value
			minimum,average = self.groom_estimated_subcatch_catch[species]			
			##I found it necessary to create temporary tables with indices on them to get adequate execution times.  Using subqueries was much slower!
			self.db.Script('''
				CREATE TEMPORARY TABLE check_ESCWN_%(species)s_g AS SELECT trip,sum(green_weight) AS sum_green_weight FROM landing WHERE species_code='%(species)s' AND trip IS NOT NULL GROUP BY trip;
				CREATE INDEX check_ESCWN_%(species)s_g_trip ON check_ESCWN_%(species)s_g(trip);
				
				CREATE TEMPORARY TABLE check_ESCWN_%(species)s_c AS SELECT trip,sum(estimated_subcatch.catch_weight) AS sum_catch_weight FROM fishing_event,estimated_subcatch WHERE fishing_event.event_key=estimated_subcatch.event_key AND species_code='%(species)s' AND trip IS NOT NULL GROUP BY trip;
				CREATE INDEX check_ESCWN_%(species)s_c_trip ON check_ESCWN_%(species)s_c(trip);
				
				DROP TABLE IF EXISTS check_ESCWN_%(species)s;
				CREATE TABLE check_ESCWN_%(species)s AS SELECT g.trip AS trip,sum_green_weight,sum_catch_weight FROM check_ESCWN_%(species)s_g AS g, check_ESCWN_%(species)s_c AS c WHERE g.trip==c.trip;
				CREATE INDEX check_ESCWN_%(species)s_trip ON check_ESCWN_%(species)s(trip);
				
				CREATE TEMPORARY TABLE check_ESCWN_%(species)s_e AS SELECT event_key FROM fishing_event WHERE trip IN (SELECT trip FROM check_ESCWN_%(species)s WHERE sum_green_weight<sum_catch_weight*%(minimum)s AND trip IS NOT NULL);
				CREATE INDEX check_ESCWN_%(species)s_e_event_key ON check_ESCWN_%(species)s_e(event_key);
				
				INSERT INTO estimated_subcatch_changes SELECT id,'CTN','catch_weight',catch_weight,catch_weight/%(average)s FROM estimated_subcatch WHERE species_code='%(species)s' AND event_key IN (SELECT event_key FROM check_ESCWN_%(species)s_e);
				UPDATE estimated_subcatch SET changed='CTN', catch_weight=catch_weight/%(average)s WHERE species_code='%(species)s' AND event_key IN (SELECT event_key FROM check_ESCWN_%(species)s_e);
				
			'''%locals())
	def summarise_(self):
		##Added _ to name so does not get used . Just a holder for some old code.
		rows = self.db.Rows('''SELECT dropped,count(*),sum(catch_weight)/1000 FROM estimated_subcatch WHERE dropped IS NOT NULL GROUP by dropped 
			UNION SELECT changed,count(*),sum(catch_weight)/1000 FROM estimated_subcatch WHERE changed IS NOT NULL GROUP BY changed;''')
		checks = (
			('CTN','Estimated catch entered as weight instead of numbers check'),
		)
		return FARTable("Summary of error checks on estimated_subcatch table. Checks are tabulated in the order that they are discussed in the following sections.",
			('Code','Description','Records','Estimated catch (t)'),rows,checks)
			
		num = self.db.Value('''SELECT count(species) FROM estimated_subcatch_CTN;''')
		if num>0:
			report += H2('''3.1 Estimated catch entered as weight instead of numbers check (CTN)''')
			report += P('''For a few species, estimated catch should be recorded in numbers rather than weights.  This check is designed to find and change those records 
			where the weight was recorded instead of numbers.  This is done by comparing the estimated catch with the landings for each trip for each species. If the ratio of landings to estimated catch
			is less than a specified threshold then the estimated catch is assumed to have been mis-reported as a catch weight and is adjusted by dividing by a specified average weight.''')

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

