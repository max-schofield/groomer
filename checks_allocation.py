from check import *

class ALLOC(Check):
	##This check does not DO anything but makes use of the 
	## Check methods like Histogram and Scatterplot to 
	## provide a summary of allocation
	brief = 'Allocation of landings to fishing events'
	desc = '''Landings are allocated to fishing events following the Starr (2010) method'''
	
	def do(self):
		pass
	
	def summarise(self):
		div = Div()
		for species in self.dataset.species:
			div += H3(species)
			
			vals = self.db.Row('''
					SELECT *
					FROM (
						SELECT 
							sum(green_weight)/1000.0
						FROM landing 
						WHERE species_code=='%s' AND fishing_year>=1990 AND dropped IS NULL
					) , (
						SELECT 
							sum(%s_est)/1000.0 AS est,
							sum(%s_prop)/1000.0 AS prop,
							CAST(sum(%s_prop_method!=0) AS REAL) AS total,
							sum(%s_prop_method==1) AS p1,
							sum(%s_prop_method==2) AS p2,
							sum(%s_prop_method==3) AS p3
						FROM fishing_event 
						WHERE fishing_year>=1990
					) ;'''%(species,species,species,species,species,species,species)
				)
			la,est,all,tot,p1,p2,p3 = vals
			div += P('''
				For the entire dataset, since 1989/90, there was a total of %it of estimated catches and %it of landings (excluding those data dropped by error checks) for %s. 
				A total of %it (%s%%) of these landings were able to be allocated to fishing events. 
				Overall %s%% of allocations were made on the basis of estimated catches, %s%% on the basis of effort and %s%% were made equally to all fishing event on the trip.
			'''%(round(est),round(la),species,round(all),round(all/la*100,2), round(p1/tot*100,1),round(p2/tot*100,1),round(p3/tot*100,1)))
				
			div += FARTable(
				'''Comparison of landings, estimated catches and allocated landings and summary of allocated method for %s. 
				Note that this summary is only for the purposes of checking the allocation of landings to fishing events, therefore
				it includes data for all trips in the dataset and will therefore will include trips that fished in other FMAs and thus will include landings for other FMAs.'''%species,
				('Fishing year','Estimated catch (t)','Landings (t)','Allocated landings (t)','Allocated using estimated (%)','Allocated using effort (%)','Allocated equally (%)'),
				self.db.Rows('''
					SELECT fishing_year,est,la,prop,p1/total*100,p2/total*100,p3/total*100
					FROM (
						SELECT 
							fishing_year,
							sum(green_weight)/1000.0 AS la
						FROM landing 
						WHERE species_code=='%s' AND fishing_year>=1990 AND dropped IS NULL
						GROUP BY fishing_year
					) INNER JOIN (
						SELECT 
							fishing_year,
							sum(%s_est)/1000.0 AS est,
							sum(%s_prop)/1000.0 AS prop,
							CAST(sum(%s_prop_method!=0) AS REAL) AS total,
							sum(%s_prop_method==1) AS p1,
							sum(%s_prop_method==2) AS p2,
							sum(%s_prop_method==3) AS p3
						FROM fishing_event 
						WHERE fishing_year>=1990
						GROUP BY fishing_year
					) USING (fishing_year);'''%(species,species,species,species,species,species,species)
				)
			)
			div += self.histogram(
				'fishing_event','%s_est/%s_prop'%(species,species),
				xlab='Estimated catches/Allocated landings',
				ylab='Events',
				where='(%s_est>0 OR %s_prop>0)'%(species,species),
				caption = '''Histogram of the ratio of allocated landings over estimated catches by fishing event for %s. This summary is for all fishing events in the dataset where either estimated catches or allocated
					landings were greater than zero.'''%species
			)
			div += self.scatterplot(
				'fishing_event','%s_est'%species,'%s_prop'%species,
				xlab='Estimated catches',
				ylab='Allocated landings',
				where='(%s_est>0 OR %s_prop>0)'%(species,species),
				lines=[(0,1)],
				caption = '''Relationship between estimated catches and allocated landings for %s. This summary is for all fishing events in the dataset where either estimated catches or allocated
					landings were greater than zero.'''%species
			)
			div += self.scatterplot(
				'fishing_event','%s_est'%species,'%s_prop'%species,
				transform='log10',
				xlab='Estimated catches',
				ylab='Allocated landings',
				where='(%s_est>0 OR %s_prop>0)'%(species,species),
				lines=[(0,1)],
				caption = '''Relationship between estimated catches and allocated landings for %s using a log10 scale. This plot allows better comparison of 
				estimated catches and allocated landings over the entire range but note that points where either estimated catches or allocated landings
				are zero can not be show. This summary is for all fishing events in the dataset.'''%species
			)
		return div