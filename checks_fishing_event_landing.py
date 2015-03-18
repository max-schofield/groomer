from check import *

class FLINI(Check):
	visible = False
	
	def do(self):
		self.db.Script('''
			DROP TABLE IF EXISTS landing_FESAU_fishstocks;
			CREATE TABLE landing_FESAU_fishstocks AS SELECT trip,fishstock_code,count(*),sum(green_weight) FROM landing GROUP BY trip,fishstock_code;
			CREATE INDEX landing_FESAU_fishstocks_trip ON landing_FESAU_fishstocks(trip);
			CREATE INDEX landing_FESAU_fishstocks_fishstock ON landing_FESAU_fishstocks(fishstock_code);
			CREATE INDEX landing_FESAU_fishstocks_trip_fishstock ON landing_FESAU_fishstocks(trip,fishstock_code);

			DROP TABLE IF EXISTS fishing_event_FESAU_stats;
			CREATE TABLE fishing_event_FESAU_stats AS SELECT trip,start_stats_area_code,count(*),sum(effort_num),sum(fishing_duration) FROM fishing_event GROUP BY trip,start_stats_area_code;
			CREATE INDEX fishing_event_FESAU_stats_trip ON fishing_event_FESAU_stats(trip);
			CREATE INDEX fishing_event_FESAU_stats_stat ON fishing_event_FESAU_stats(start_stats_area_code);
			CREATE INDEX fishing_event_FESAU_stats_trip_stat ON fishing_event_FESAU_stats(trip,start_stats_area_code);
		''')
		
	def report(self):
		return ''
	
class FLSAU(Check):
	brief = 'Statistical area unmatched by landing'
	desc = '''		
		Check that the recorded combinations of <i>landing.fishstock_code</i> and <i>fishing_event.start_stats_area_code</i> are valid:
		<ul>
			<li>each <i>fishstock_code</i> has a corresponding <i>start_stats_area_code</i>
			<li>each <i>start_stats_area_code</i> has a corresponding <i>fishstock_code</i>
		</ul>
		Apart from being inconsistent, these trips need to be dropped because landings can not be allocated properly when there is no match between 
		Fishstock and the statistical area for the trip.
	'''
	table = 'fishing_event'
	column = 'start_stats_area_code'
	
	def do(self):
		##For each species...
		print
		for species in self.db.Values('''SELECT species_code FROM landing GROUP BY species_code HAVING count(*)>=1000;'''):
			print species
			##Check that all recorded stats have corresponding fishstock in landing
			##This needs to be done by species since (a) the stats-fishstock relationship varies by species (b) the need to restrict to target events because
			##just because there was effort in in area does not mean that you catch the species
			for stat in self.db.Values('''SELECT DISTINCT start_stats_area_code FROM fishing_event WHERE target_species=='%s' AND start_stats_area_code IS NOT NULL;'''%species):
				print '  ',stat
				for trip in self.db.Values('''SELECT DISTINCT trip FROM fishing_event WHERE target_species=='%s' AND start_stats_area_code=='%s' AND trip IS NOT NULL;'''%(species,stat)):
					if self.db.Value('''SELECT count(*) FROM landing_FESAU_fishstocks WHERE trip==%s AND fishstock_code IN (SELECT qma FROM qmastats WHERE species=='%s' AND stat=='%s');'''%(trip,species,stat))==0:
						self.flag(clause='''trip==%s AND target_species=='%s' AND start_stats_area_code=='%s' '''%(trip,species,stat))
	
	def summarise__(self):
		div = Div()
		div += FARTable(
			'Errors by fishstock',
			('Fishstock','Records','Landings (t)'),
			self.db.Rows('''SELECT fishstock_code,count(*),sum(green_weight)/1000 FROM landing WHERE flags LIKE '%FLSAU%' GROUP BY fishstock_code''')
		)
		return div
		
		div += FARTable(
			'For trips with FSM errors the stat area recorded by fishstock (limited to 100)',
			('Fishstock','Stat area','Trips'),
			self.db.Rows('''SELECT fishstock_code,start_stats_area_code,count(*) FROM landing_FSM GROUP BY fishstock_code,start_stats_area_code ORDER BY count(*) DESC LIMIT 100;''')
		)
		div += FARTable(
			'For trips with FSM errors summary of the port of landing (limited to 250)',
			('Fishstock','Stat area','Port','Count'),
			self.db.Rows('''
				SELECT lfsm.fishstock_code,start_stats_area_code,landing_name,count(*) 
				FROM landing_FSM AS lfsm LEFT JOIN landing USING (trip) 
				GROUP BY lfsm.fishstock_code,start_stats_area_code,landing_name 
				ORDER BY count(*) DESC LIMIT 250;'''
			)
		)
		return div

class FLFSU(Check):
	brief = 'Fishstock unmatched by effort'
	desc = '''	'''
	table = 'landing'
	column = 'fishstock_code'
	
	species = None
	fishstocks = None
	
	def do(self):
		##For each species...
		print
		if self.species is None: species_list = self.db.Values('''SELECT species_code FROM landing GROUP BY species_code HAVING count(*)>=1000;''')
		else: species_list = self.species
		for species in species_list:
			print species
			##..for each fishstock...
			if self.fishstocks is None: fishstocks = self.db.Values('''SELECT DISTINCT fishstock_code FROM landing WHERE species_code=? AND fishstock_code IS NOT NULL;''',[species])
			else: fishstocks = self.fishstocks
			for fishstock in fishstocks:
				print '  ',fishstock
				##..for each trip recording that fishstock...
				for trip in self.db.Values('''SELECT DISTINCT trip FROM landing WHERE fishstock_code==? AND trip IS NOT NULL;''',[fishstock]):
					##..check that trip has a corresponding stat for that trip
					if self.db.Value('''SELECT count(*) FROM fishing_event_FESAU_stats WHERE trip==%s AND start_stats_area_code IN (SELECT stat FROM qmastats WHERE qma=='%s');'''%(trip,fishstock))==0:
						self.flag(clause='''trip==%s AND fishstock_code=='%s' '''%(trip,fishstock))
