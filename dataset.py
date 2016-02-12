import sys,os,sqlite3,string,datetime,random,copy

from collections import OrderedDict

from database import Database
from checks import *

version = 1.3

# Home directory of groomer used below to read files
home = os.path.dirname(os.path.realpath(__file__))

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
            'extract_cpue_species':None,
            'extract_cpue_catches':['est','prop'], ##More than one field can be extracted to cpue.txt file
            
            'zones':None,
            
            'scale_to_totals': True,
            
            'extra_fields':[], ##Additional field to carry over from fishing_event to simple.db3 and cpue.txt
            'extra_factors':[] ##Additonal factors for Tanga:Web schema  e.g. [('port_stat',0,1)]
            
        }.items(): setattr(self,kw,arg)

        ##Set defaults for specified extract filenames
        self.extract_filenames = {
            'trip_details':None,
            'fishing_event':None,
            'estimated_subcatch':None,
            'processed_catch':None,
            'landing':None,
            'vessel_history':None,
            'mhr':None,
        }

        ##Apply overrides specified in arguments
        for kw,arg in kwargs.items(): setattr(self,kw,arg)
            
        ##Give a name based on name of current working directory
        if not hasattr(self,"name"): self.name = os.getcwd().split('/')[-1]
        
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

We have been contracted by %s under project "%s" to analyse catch and effort data for %s. We would like
to do an initial summary of the data for these Fishstocks so that we can determine the best set of criteria for our data extract.
Please could you do a count of the number of fishing_events recording each primary_method, target_species, start_stats_area_code
and form_type for all of the trips that landed to these Fishstocks between %s and %s.'''%(self.client,self.project,fishstocks,self.begin,self.end)

        email += '''
\n\nAppended to the end of this message is draft SQL that should do the summaries required.  This SQL is
based on the SQL that we have received from MFish in previous extracts. It involves the creation of a temporary table which lists the qualifying trips
and extracts of various data based on this table. Could you please supply us with the final SQL used, so that for future extracts we can
correct any errors or omissions that we have made.

The request is subject to the conditions of the existing confidentiality agreement between MPI and us.
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
create table tempdb..%s (trip keys null);
go
create index index_1 on tempdb..%s (trip);
go
        '''%(self.name,self.name)

        sql += '''
/* Insert trips that meet landings criteria*/
insert tempdb..%s
select distinct la.trip
from warehou..ce_landing la
where la.interp_yn = 'Y'
and la.landing_datetime >= '%s'
and la.landing_datetime < '%s' 
and la.species_code in (%s)
        '''%(self.name,self.begin,self.end,species_quoted)
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
from warehou..ce_fishing_event fi,tempdb..%s tt
where fi.interp_yn = 'Y' and tt.trip = fi.trip
group by fi.%s;
            '''%(factor,factor,self.name,factor)

        file('prerequest.txt','w').write(email+'\n\n'+sql+'\n\n')

    def request(self):
        '''Automatically create a data request email based'''
        
        species = ', '.join(self.species)
        species_quoted = ','.join([repr(item) for item in self.species])
        fishstocks_list = []
        for fishstocks in self.fishstocks.values(): fishstocks_list.extend(fishstocks)
        fishstocks  = ', '.join(fishstocks_list)
        fishstocks_quoted = ','.join([repr(item) for item in fishstocks_list])

        ##Email text
        email = '''\nDear RDM,\n\nWe have been contracted by %s under project "%s" to analyse catch and effort data for %s. We would like to obtain catch and effort data for fishing trips that occurred between %s and %s, and which'''%(self.client,self.project,fishstocks,self.begin,self.end)

        if len(fishstocks_list)>0:  email += ''' landed to %s '''%(fishstocks)

        criteria = []
        if len(self.statareas): criteria.append('''  - were in statistical area(s) %s,'''%(', '.join(self.statareas)))
        if len(self.methods): criteria.append('''  - used method(s) %s,'''%(', '.join(self.methods)))
        if len(self.targets): criteria.append('''  - targeted species %s'''%(', '.join(self.targets)))
        if hasattr(self,'targets_not') and len(self.targets_not): criteria.append('''  - did not target species %s'''%(', '.join(self.targets_not)))
        if hasattr(self,'extra_criteria') and len(self.extra_criteria): criteria.append('''  - where %s'''%self.extra_criteria)
        if len(criteria)>0:
            email += ', or which had fishing events that:\n'
            email += ' and,\n'.join(criteria)

        email += '\n\n'
        email += 'For these trips we would like to obtain all effort, landings and estimated catch data. '
        email += 'In addition, we would like to obtain monthly harvest return (MHR) data for all available months for the above fishstocks by month, client and Fishstock.'

        email += '\n\n'
        email += 'Attached to this email is draft SQL that includes the fields required for this extract. '
        email += 'It involves the creation of a temporary table which lists trips which qualify according to the above criteria and subsequently extracts various data based on this table. '
        email += 'Could you please supply us with the final SQL used, so that for future extracts we can correct any errors or omissions that we have made. '
        email += 'Please could you provide the data in the usual fixed field width text format.'

        if self.request_extra_notes: email += '\n\n%s\n\n'%self.request_extra_notes

        email += '\n\n'
        email += 'This request is subject to the conditions of the existing confidentiality agreement between MPI and us. '
        email += 'Please don\'t hesitate to contact me if you require any further details.'
    
        ##Separate files for each extract file
        tt = '''
/* **************************************************/
/*    List of trips that define the extract         */
/* **************************************************/
/* Create temporary table with index on trip */

use tempdb
go

create table tempdb..%s (trip keys null)
go

create index index_1 on tempdb..%s (trip)
go
        '''%(self.name,self.name)

        tt += '''
insert tempdb..%s

/* Insert trips that meet landings criteria*/
select distinct la.trip
from warehou..ce_landing la
where la.interp_yn = 'Y'
  and la.landing_datetime >= '%s'
  and la.landing_datetime < '%s' '''%(self.name,self.begin,self.end)
        if len(fishstocks_list)>0: tt += '''\n  and la.fishstock_code in (%s)'''%fishstocks_quoted
        else: tt += '''\n  and la.species_code in (%s)'''%species_quoted

        tt += '''\n\nunion\n'''

        if len(criteria)>0:
            tt += '''
/* Insert trips that meet effort criteria*/
select distinct fi.trip
from warehou..ce_fishing_event fi  
where fi.interp_yn = 'Y'
  and fi.start_datetime >= '%s'
  and fi.start_datetime < '%s' '''%(self.begin,self.end)

            if len(self.statareas)>0: tt += '''\n  and fi.start_stats_area_code in (%s)'''%','.join([repr(item) for item in self.statareas])
            if len(self.methods)>0: tt += '''\n  and fi.primary_method in (%s)'''%','.join([repr(item) for item in self.methods])
            if len(self.targets)>0: tt += '''\n  and fi.target_species in (%s)'''%','.join([repr(item) for item in self.targets])
            if hasattr(self,'targets_not') and len(self.targets_not)>0: tt += '''\n  and fi.target_species not in (%s)'''%','.join([repr(item) for item in self.targets_not])
            if hasattr(self,'extra_criteria') and len(self.extra_criteria)>0: tt += '''\n  and %s'''%self.extra_criteria
                
            tt += '''\n\ngo\n'''

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
from warehou..ce_trip_details td,tempdb..%s tt
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
  fi.fishing_duration,
  fi.catch_weight,
  fi.catch_weight_other,
  fi.non_fish_yn,
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
  fi.start_latitude,
  fi.start_longitude,
  fi.end_latitude,
  fi.end_longitude,
  fi.start_stats_area_code,
  fi.vessel_key,
  fi.client_key,
  fi.dcf_key ,
  fi.form_type,
  fi.trip
from warehou..ce_fishing_event fi,tempdb..%s tt
where fi.interp_yn = 'Y' and tt.trip = fi.trip
go
        '''%(self.name)

        ca = '''
/* *********************************************/
/*      Estimated catch for all species        */
/* *********************************************/
select 
  ca.event_key,
  ca.version_seqno,
  ca.group_key,
  ca.species_code,
  ca.catch_weight,
  ca.trip
from warehou..ce_estimated_subcatch ca,tempdb..%s tt
where ca.interp_yn = 'Y'
  and tt.trip = ca.trip
go
        '''%(self.name)
            
        pr = '''
/* *********************************************/
/*      Processed catch for all species        */
/* *********************************************/
select 
  pr.event_key,
  pr.version_seqno,
  pr.group_key,
  pr.specprod_seqno,
  pr.specprod_action_type,
  pr.processed_datetime,
  pr.species_code,
  pr.state_code,
  pr.unit_type,
  pr.unit_num,
  pr.unit_weight,
  pr.conv_factor,
  pr.processed_weight,
  pr.processed_weight_type,
  pr.green_weight,
  pr.green_weight_type,
  pr.dcf_key,
  pr.form_type,
  pr.trip
from warehou..ce_processed_catch pr,tempdb..%s tt
where pr.interp_yn = 'Y'
  and tt.trip = pr.trip
go
        '''%(self.name)
            
        la = '''
/* *********************************************/
/*      Landings for all species               */
/* *********************************************/
select 
  la.event_key,
  la.version_seqno,
  la.group_key,
  la.specprod_seqno,
  la.landing_datetime,
  la.landing_name,
  la.species_code,
  la.fishstock_code,
  la.state_code,
  la.destination_type,
  la.unit_type,
  la.unit_num,
  la.unit_num_latest,
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
from warehou..ce_landing la, tempdb..%s tt
where la.interp_yn = 'Y'
  and tt.trip = la.trip
go
        '''%(self.name)

        vs = '''
/* ************************************************************/
/*      Vessel histories for vessels in the selected trips    */
/* ************************************************************/
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
from corporat..vs_vessel_history vs
where vs.history_end_datetime >= '%s'
  and vs.history_start_datetime < '%s'
  and vs.vessel_key in (select distinct vessel_key
  from warehou..ce_trip_details td,tempdb..%s tt
  where tt.trip = td.trip)
go
        '''%(self.begin,self.end,self.name)

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
  and mq.from_datetime < '%s'
  and mq.stock_code in (%s)
go
        '''%(self.end,fishstocks_quoted)

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
            file('request/%s_%s.%s'%(self.name,item,ext),'w').write(content)

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
            if ".tab" in filename or ".tsv" in filename: format = "\t"
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
                if value=='NULL' or value=='[NULL]' or value=='': value = None
                elif 'datetime' in names[index]:
                    value = value.replace('a.m.','AM').replace('p.m.','PM')
                    if self.extract_datetime_format==1:
                        value = datetime.datetime.strptime(value,'%b %d %Y %I:%M%p')
                    elif self.extract_datetime_format==2:
                        # Deal with some extract dates being %Y-%m-%d and some being %Y-%m-%d %H:%M:%S
                        value = datetime.datetime.strptime(value,'%Y-%m-%d') if len(value)==10 else datetime.datetime.strptime(value,'%Y-%m-%d %H:%M:%S')
                    elif type(self.extract_datetime_format) is str:
                        #Deal with dates of format"Feb  9 1993  6:10:00:000AM"
                        value = datetime.datetime.strptime(value,self.extract_datetime_format)
                    elif type(self.extract_datetime_format) is dict:
                        dt_format = self.extract_datetime_format.get(table)
                        if dt_format is None: dt_format = self.extract_datetime_format.get('_default_')
                        value = datetime.datetime.strptime(value,dt_format)
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
        # Delete the existing database
        if os.path.exists('database.db3'): os.remove('database.db3')
            
        # Create a new db connection and initialise it
        self.db = Database('database.db3')
        
        # Create the database schema
        self.db.Script(file(os.path.join(home,'dataset.sql')).read())
            
        # Load in extract files
        self.loadFile('trip_details',filename=self.extract_filenames['trip_details'])
        self.loadFile('fishing_event',filename=self.extract_filenames['fishing_event'],filetags=['effort','fish_events'])
        self.loadFile('estimated_subcatch',filename=self.extract_filenames['estimated_subcatch'],filetags=['estimated_catch','est_catch','estcatch'])
        self.loadFile('processed_catch',filename=self.extract_filenames['processed_catch'],filetags=['processed catch','proc_catch','procatch'])
        self.loadFile('landing',filename=self.extract_filenames['landing'],filetags=['landed_catch'])
        self.loadFile('vessel_history',filename=self.extract_filenames['vessel_history'],filetags=['vessel_specs','vessel_spx','vessel_spexs','vessel_specx'])
        self.loadFile('mhr',filename=self.extract_filenames['mhr'])

        # Load in files used for checks
        self.loadFile('dqss',filename=os.path.join(home,'dqss_range_checks.tsv'))
        self.loadFile('qmastats',filename=os.path.join(home,'qma_stats.tsv'))
        self.loadFile('stats_boxes',filename=os.path.join(home,'stats_boxes.tsv'))

        # Use default stats for QMA if needed
        for species in self.species:
            if self.db.Value('''SELECT count(*) FROM qmastats WHERE species=='%s';'''%species)==0:
                print 'No stats in qmastats for species %s. Using stats for "%s".'%(species,self.load_qmastats)
                self.db.Execute('''INSERT INTO qmastats(species,qma,stat) SELECT '%s','%s'||(CASE length(qma)>4 WHEN 1 THEN substr(qma,4,2) ELSE substr(qma,4,1) END),stat FROM qmastats WHERE species=='%s';'''%(species,species,self.load_qmastats))

        # Load QMR data (this is the same across datatsets) so should be in the groomer home
        # directory.
        try: self.loadFile('qmr',filename=os.path.join(home,'qmr.txt'))
        except Exception, e: print e,'Ignoring'

        # Load catch history data
        try: self.loadFile('history',filename='history.csv')
        except Exception, e: print e,'Ignoring'

        self.db.Execute('''INSERT INTO status(task,done) VALUES('load',datetime('now'));''')
        self.db.Commit()
    
    def groom(self):
        Check.dataset = self
        Check.db = self.db
        Check.applyAll(force=True)
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
            
            ##Use the defined fishstocks for allocations, or if not defined all fishstocks reported
            fishstocks = self.fishstocks.get(species,None)
            if fishstocks is None: fishstocks = self.db.Values('''SELECT DISTINCT fishstock_code FROM landing WHERE species_code='%s' '''%species)
            ##Starr E.1.5 & E.1.6 & F Allocate landings to effort_collapsed strata
            ##Allocate each trip/fishstock stratum in landings_collapsed to matching trip/stat/method/target stratums in effort collapsed
            ##Allocation is done on two bases (a) equally divided (b) relative to estimated catch (or on the basis of effort_num if there was no estimated catch,
            ## or equally if a mixed method trip or no effort_num
            ##Allocate landings for each fishstock...
            for fishstock in fishstocks:
                stats = ','.join([repr(str(value)) for value in self.db.Values('''SELECT stat FROM qmastats WHERE qma='%s' '''%fishstock)])
                print fishstock,stats
                trips_tried,trips_failed = 0,0
                ##...for each trip...
                for trip,sum_green_weight in self.db.Rows('''SELECT trip,sum(green_weight) FROM landing WHERE fishstock_code='%s' AND dropped IS NULL AND green_weight> 0 AND trip IS NOT NULL GROUP BY trip;'''%fishstock):
                    trips_tried+= 1
                    if trips_tried%1000==0: 
                        sys.stdout.write('.')
                        sys.stdout.flush()
                    count,sum_effort_num,sum_catch_weight = self.db.Row('''SELECT count(*),sum(effort_num),sum(%s_est) FROM fishing_event WHERE trip=%s AND start_stats_area_code IN (%s)'''%(species,trip,stats))
                    if sum_effort_num is None: sum_effort_num = 0
                    if sum_catch_weight is None: sum_catch_weight = 0
                    if count==0: trips_failed += 1
                    else:
                        equ = sum_green_weight/float(count)
                        self.db.Execute('''UPDATE fishing_event SET %s_equ=%s_equ+%s WHERE trip=%s AND start_stats_area_code IN (%s)'''%(species,species,equ,trip,stats))
                        if sum_catch_weight>0:
                            for event_key,catch_weight in self.db.Rows('''SELECT event_key,%s_est FROM fishing_event WHERE trip=%s AND start_stats_area_code IN (%s);'''%(species,trip,stats)):
                                if catch_weight is None: catch_weight = 0
                                prop = sum_green_weight * catch_weight/float(sum_catch_weight)
                                self.db.Execute('''UPDATE fishing_event SET %s_prop=%s_prop+%s,%s_fishstocks=%s_fishstocks+1,%s_prop_method=1 WHERE event_key=%s;'''%(species,species,prop,species,species,species,event_key))
                        else:
                            methods = self.db.Values('''SELECT DISTINCT primary_method FROM fishing_event WHERE trip=%s AND start_stats_area_code IN (%s)'''%(trip,stats))
                            if len(methods)==1 and sum_effort_num>0:
                                for event_key,effort_num in self.db.Rows('''SELECT event_key,effort_num FROM fishing_event WHERE trip=%s AND start_stats_area_code IN (%s)'''%(trip,stats)):
                                    if effort_num is None: effort_num = 0
                                    prop = sum_green_weight * effort_num/float(sum_effort_num)
                                    self.db.Execute('''UPDATE fishing_event SET %s_prop=%s_prop+%s,%s_fishstocks=%s_fishstocks+1,%s_prop_method=2 WHERE event_key=%s'''%(species,species,prop,species,species,species,event_key))
                            else: self.db.Execute('''UPDATE fishing_event SET %s_prop=%s_prop+%s,%s_fishstocks=%s_fishstocks+1,%s_prop_method=3 WHERE trip=%s AND start_stats_area_code IN (%s)'''%(species,species,equ,species,species,species,trip,stats))
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
            zones = file(os.path.join(home,"stats_zones.tsv"))
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
        species_list = copy.copy(self.species)
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
            start_latitude AS lat,
            start_longitude AS lon,
            CAST (start_latitude AS INTEGER) AS latd,
            CAST (start_longitude AS INTEGER) AS lond,
            CAST (start_latitude/0.2 AS INTEGER) * 0.2 AS latd2,
            CAST (start_longitude/0.2 AS INTEGER) * 0.2 AS lond2,
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
        WHERE
            year IS NOT NULL AND
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
                ##Can't do it for species where there are no fishstocks defined and so no value to scale up to
                if not self.fishstocks.has_key(species): continue
                
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
        for factor,order,maxi in ([('year',0,1),('method',1,1),('target',1,1),('area',0,1),('zone',0,1),('form',1,1)]+self.extra_factors):
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
                
        ##Create a vessels table
        self.db.Execute('''DROP TABLE IF EXISTS vessels;''')
        self.db.Execute('''CREATE TABLE vessels AS 
            SELECT vessel_key,%s ,%s
            FROM vessel_history
            GROUP BY vessel_key;'''%(
                ','.join(['median(CAST(%s AS REAL)) AS %s'%(name,name) for name in (
                        'overall_length_metres',
                        'registered_length_metres',
                        'draught_metres',
                        'beam_metres',
                        'gross_tonnes',
                        'max_speed_knots',
                        'service_speed_knots',
                        'engine_kilowatts',
                        'max_duration_days'
                    )]),
                ','.join(['mode(%s) AS %s'%(name,name) for name in (
                        'flag_nationality_code',
                        'built_year',
                        'tenders_number',
                        'total_crew_number',
                        'base_region_code',
                        'base_port_code'
                    )])
        ))
        self.db.Execute('''CREATE INDEX vessels_vessel_key ON vessels(vessel_key);''')
        ##Dump to text file
        out = file('vessels.txt','w')
        self.db.Execute('''SELECT * FROM vessels LIMIT 1;''')
        print>>out, '\t'.join(self.db.Fields())
        for row in self.db.Rows('''SELECT * FROM vessels;'''): print>>out, '\t'.join((str(item) if item is not None else 'NA') for item in row)
                
        ##Save to a smaller database
        simple_path = 'simple.db3'
        if os.path.exists(simple_path): os.remove(simple_path)
        simple = Database(simple_path)
        simple.Execute('''ATTACH DATABASE "%s" AS original;'''%('database.db3'))
        simple.Execute('''CREATE TABLE events AS SELECT * FROM original.events;''')
        simple.Execute('''CREATE TABLE vessels AS SELECT * FROM original.vessels;''')
                
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
        ##Starr paragraph D.2.3 says that for CPUE studies  trips which have data changed under D.2.4 (paragraph D.2.2) should be dropped
        ##unless the form type is TCPER. I create a new field which acts as a field for whether the record should be used in CPUE analyses
        self.db.Alter('''ALTER TABLE fishing_event ADD COLUMN cpue_no INTEGER;''')
        for flag in ('FEEFO',): self.db.Execute('''UPDATE fishing_event SET cpue_no=1 WHERE flags LIKE '%%%s%%';'''%flag)

        species_fields = ''
        for species in (self.extract_cpue_species if self.extract_cpue_species is not None else species_list):
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
            start_latitude AS lat,
            start_longitude AS lon,
            
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
            
            /* Grooming flags */
            flags AS flags,
            /* Is this record suitable for CPUE analysis? See dataset.py for when this is set */
            cpue_no AS cpueno
            
            /* 
            * Catches allocated (and auxillary information from allocation) to each fishing_event for each allocated species:
            *   _est: estimated catches for this fishing_event summed from the estimated_subcatches table
            *   _equ: landings allocated equally to all fishing_events in a trip
            *   _prop: landings allocated to fishing_events in a trip in proportion to estimated catches or effort (see below for more details)
            *   _prop_method: the method used for _prop 1=proportional to _est; 2=no _est for trip so proportional to effort_num
            *   _fishstocks: the number of fishstocks for this species that the trip landed to
            */
            %s
            
        FROM fishing_event
        WHERE  
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
