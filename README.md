# groomer

### What and why

A system for requesting, loading, grooming and merging New Zealand fisheries catch and effort data. We use `groomer` at [Trophia](http://www.trophia.com) in analyses of catch and effort data.

Groomer was initially developed in 2008 to implement the grooming and merging algorithms described in Starr (2007) (and subsequent revisions). In 2011, as part of the "Tanga" family of tools used by Trophia for analyses of catch and effort data, it was dubbed `tangagroomer` (the other tools being `tangaviewer` and `tangaanalyser`) and setup as a [Mercurial](http://mercurial.selenic.com/) repository. It has been used extensively by some Trophia associates (others use R or Stata versions) but has not had any major development since 2012. In March 2015, `tangagroomer` was renamed `groomer`, switched over to a [Git](http://git-scm.com) repository, and open sourced under the [GPLv3 licence](LICENCE.txt).

This software was developed more than six years ago. If we were to write this again we would do things differently. We don't plan on doing a major revamp and we don't necessarily recommend that anyone else use it. Our intension in open sourcing `groomer` is simply to provide others in the NZ fisheries science community with ideas for approaches and techniques - please feel free to take the good bits for your own project and leave the bad. Specifically, we are hoping that there might be something in here that is useful for [Trident's](http://www.tridentsystems.co.nz/) `kahawhai` database and hope to be able to abandon `groomer` in favour of collectively contributing to, and making use of, `kahawai`. 

There is not a lot of documentation other than this README and the documentation strings and comments in the code. If you would like advice on what exactly the "good bits" are, please contact [Nokome](http://trophia.com/team/nokome-bentley/)...or read on. You probably won't get much help if all you want to do it run this thing. Like it says above, that's not why this is here.

Starr, P.J. (2007). Procedure for merging MFish Landing and Effort data. V2.0. Unpublished report held by Ministry for Primary Industries as document AMPWG 07/04. 17 p.

### Overview

Groomer uses Python and SQLite. Most of the heavy lifting numerical stuff is done in SQLite. Python provides the "frontend" API, generates some of the SQL, and generates the HTML summary reports.

The cornerstone of `groomer` is the `Dataset` class. For each project, you define a `Dataset` in a python file (e.g. `dataset.py`) and then run its methods at various times to perform various operations on it. Most of these methods produce output directories or files. For example, the `request()` method creates a `request` directory, and the `load()` method creates `database.db3` from the files in `extract`. After running some of the methods your project directory might look like this:

```
project/
├── dataset.py
├── database.db3
├── extract
├── request
└── summary
```

A typical `dataset.py` looks something like this:

```py
from groomer.dataset import Dataset

dataset = Dataset(
	name = 'bbf9',
	client = 'New Zealand Ministry of Primary Industries',
	project = 'BFF2015-01 : Characterization of big bluefish in BBF 9',

	begin = '01 Oct 1989',
	end = '01 Oct 2014',
	species = ['BFF'],
	fishstocks = {
		'BFF':['BFF9'],
	},
	statareas = ['091','092','099'],
	methods = ['BT','BLL'],
	targets = ['BBF','BNS','ALB']
)

dataset.request()
dataset.load()
dataset.groom()
dataset.allocate()
dataset.augment()
dataset.simplify()
dataset.summarize()

```

As we will see the attributes of the `Dataset` determine the extract criteria and to some extent the grooming performed on it.

### Request method

The `request()` method generates a data extract request in folder `request` with files prefixed with the name of the dataset:

```
request/
├── bbf9_ca.sql
├── bbf9_email.txt
├── bbf9_fi.sql
├── bbf9_la.sql
├── bbf9_mq.sql
├── bbf9_pr.sql
├── bbf9_td.sql
├── bbf9_tt.sql
└── bbf9_vs.sql
```

The `email.txt` file contains draft text for an email to send to RDM. It describes the extract criteria, as specified in the `Dataset` definition, in words:

```
Dear RDM,

We have been contracted by New Zealand Ministry of Primary Industries under project
"BFF2015-01 : Characterization of big bluefish in BBF 9" to analyse catch and effort
data for BFF9. We would like to obtain catch and effort data for fishing trips that
occurred between 01 Oct 1989 and 01 Oct 2014, and which landed to BFF9 , or which 
had fishing events that:

  - were in statistical area(s) 091, 092, 099, and,
  - used method(s) BT, BLL, and,
  - targeted species BBF, BNS, ALB
...
```

The `.sql` files are also generated based on the `Dataset` definition. The most important, and the one which the other SQLs are based, is the `tt.sql` which creates a temporary table which lists the trips that qualify for the dataset:

```sql
/* **************************************************/
/*    List of trips that define the extract         */
/* **************************************************/
/* Create temporary table with index on trip */

use tempdb
go

create table tempdb..bbf9 (trip keys null)
go

create index index_1 on tempdb..bbf9 (trip)
go
        
insert tempdb..bbf9

/* Insert trips that meet landings criteria*/
select distinct la.trip
from warehou..ce_landing la
where la.interp_yn = 'Y'
  and la.landing_datetime >= '01 Oct 1989'
  and la.landing_datetime < '01 Oct 2014' 
  and la.fishstock_code in ('BFF9')

union

/* Insert trips that meet effort criteria*/
select distinct fi.trip
from warehou..ce_fishing_event fi  
where fi.interp_yn = 'Y'
  and fi.start_datetime >= '01 Oct 1989'
  and fi.start_datetime < '01 Oct 2014' 
  and fi.start_stats_area_code in ('091','092','099')
  and fi.primary_method in ('BT','BLL')
  and fi.target_species in ('BBF','BNS','ALB')

go
```

### Load method

When you get the extract back from MPI you put it into a subdirectory called `extract` and run the `load()` method. This creates a SQLite database, imaginatively called `database.db3`, in the project directory. The schema for this database is in [dataset.sql](dataset.sql) which mirror the tables in MPI's `warehou` database and add some additional tables used in error checking. The key tables that the extract files are loaded into are `trip_details`, `fishing_event`, `estimated_subcatch`, `processed_catch`, `landing`, `vessel_history`, `mhr` and `qmr`.

### Groom method

The `groom()` method runs a series of checks. Each check is defined as a Python class derived from the `Check` class (defined in [check.py](check.py)) with a five letter name. The first two letters signify the table/s that the check relates to and the last three what the check checks for. Checks are grouped into files by table:

Name prefix   | Table/s                          |    File
--------------|----------------------------------|---------------------------
TD            | `trip_details`                   | [trip_details.py](trip_details.py)
FE            | `fishing_event`                  | [checks_fishing_event.py](checks_fishing_event.py)
ES            | `estimated_subcatch`             | [checks_estimated_subcatch.py](checks_estimated_subcatch.py)
LA            | `landing`                        | [checks_landing.py](checks_landing.py)
FL            | `fishing_event` & `landing`      | [checks_fishing_event_landing.py](checks_fishing_event_landing.py)

The `Check` base class defines some useful methods that allow checks to be defined and documented succinctly. For example, the `FESDM` which check for missing start date/time in the `fishing_event` table is defined and documented as:

```py
class FESDM(FE):
	brief = 'Start date/time is missing'
	desc = '''
		The starting date/time for a fishing event can be missing. This check flags those records but no attempt is made to impute the date/time.
	'''
	column = 'start_datetime'
	clause = '''start_datetime IS NULL'''
```

Other checks are more involved and implement a `do()` method that runs SQL in the database. For example,

```py
class FEFMA(FE):
	brief = 'Fisheries management area ambiguous'
	desc = '''
		Starr (2011) suggest to mark trips which landed to more than one fishstock for straddling statistical areas. Since this relies on fishing event.start_stats_area_code do this check after grooming on that.
	'''
	
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
```

There are two special checks, `CHINI` and `CHSTA`. `CHINI` is the first check conducted when `groom()` is called. It creates two tables for recording error checking actions:

- the `checking` table records which check were done and when
- the `checks` table is used for recording, and potentially rolling back, any changes made to table fields; it records the table, column and row id and original and new versions of the data:

	```sql
	CREATE TABLE IF NOT EXISTS checks (
		code TEXT,
		"table" TEXT,
		"column" TEXT,
		id INTEGER, 
		details TEXT, 
		orig TEXT, 
		new TEXT
	);
	```

`CHSTA`, or its derivatives, is the first check run for each table. It adds a `flags` text column to a table. If a record is marked by a check then that check's five letter code is added to `flags`. For example, a row in the `fishing_event` table might have a value for `flags` of `FESDM, FEPMI`.

When a check is run it can perform either a `flag` or a `change` on a record. Flags get recorded in the `flags` field, changes get recorded in the `checks` table.

The file [checks.py](checks.py) collects all the check classes together into an array inevitably called `Check.List` which the `Dataset.groom()` method iterates over. Currently, the complete check list looks like this:

```
Check.List = [
	
	CHINI,
	
	TDSTA,
	TDDAB,
	
	ESSTA,
	#ESCWN, ##Not fully operational at this stage
	
	FESTA,
	FESDM,
	FESDF,
	FEPMI,
	FEPMM,
	FETSE,
	FETSW,
	FETSI,
	FETSM,
	FESAI,
	FESAM,
	FELLI,
	FELLS,
	FEFMA,
	FEETN,
	FEEFO,
	
	LASTA,
	LADAM,
	LADAF,
	LADTI,
	LADTH,
	LADTT,
	LASCF,
	LASCI,
	LASCD,
	LACFM,
	LACFC,
	LADUP,
	LAGWI,
	LAGWM,
	LAGWR,
	
	FLINI,
	FLSAU,
	#FLFSU,
]
```

### Allocate method

The `allocate()` method does what Starr (2007) called "merging" - it allocates landed catches (recorded at the trip level; stored in the `landing` table) to fishing events (in the `fishing_event` table). It does this using one of three methods:

1. in proportion to estimated catches for the species, if any, otherwise,
2. in proportion to number of effort units (`effort_num`), if a single method trip, otherwise,
3. equally across all events for the trip.

For each species for which landing are allocated, the `allocate()` method adds five new columns to the `fishing_event` table (where `XXX` is the species code):

- `XXX_est` - total catch in the `estimated_subcatch` table for the species for the event
- `XXX_equ` - the trip landings for the species allocated equally across events
- `XXX_prop` - the trip landings for the species allocated to the event proportionally according to the above method
- `XXX_prop_method` - the allocation method used; 1, 2, or 3 as above
- `XXX_fishstocks` - the number of Fishstocks for the species for which landing were allocated (for events that occur in statistical areas that straddle QMA boundaries)

### Augment method

The `augment()` adds columns that may be useful in further analyses to several tables once grooming and allocation is done. "Highlights" include:

- a `fishing_year` column to tables where this does not exist
- a `vessel_day` column for `fishing_event` which is the inverse of the number of events for that vessel/date
- a `zone` column for `fishing_event` based on the statistical area and a user supplied mapping of stat area to zone

### Simplify method

The `simplify()` method creates a simplified version of the database for use in characterisations. This method was designed for producing a stripped down dataset for consumption by the `tangaviewer` browser based analysis tool and a CPUE dataset for consumption by the `tangaanalyser` tool.

Amongst other things, `simplify()` does:

- hashes `vessel_keys` for obfuscation
- creates a table `events` which is a near copy of `fishing_event` but with shorter names for easier typing
- scales the landed catches up to the totals for the QMA in the fishing year based on QMR/MHR data
- collapses minor levels of categorical variables into an `Other` level.
- outputs a text file with `fishing_event` records suitable for CPUE analyses (those which do not have any effort grooming)

### Summarize method

A dataset's `summarise()` method, produces a HTML report with tables and figures that summarizes the error checking and allocation done. The method iterates over the `Check.List` and for each check adds sections with headings and introductory text using the `brief` and `desc` attributes (as shown for `FESDM` above). In addition, it and calls the `summarise()` method of each check which returns a HTML `<div>` element that is appended to the report. The base `Check` class has a `summarise()` method which produces a table of the records flagged or changed by the check. This can be overridden to produce custom summaries, including figures, for specific checks. For example, the `LADUP` check which checks for landing duplicated on both CELR and CER forms, has this `summarise()` method:

```py
	def summarise(self):
		div = Check.summarise(self)
		div += FARTable(
			'''%s Errors by fishstock_code,state_code,destination_type'''%self.code(),
			('Fishstock','State','Destination','Records','Landings (t)'),
			self.db.Rows('''SELECT fishstock_code,state_code,destination_type,count(*),sum(green_weight)/1000 FROM landing WHERE flags LIKE '%LADUP%' GROUP BY fishstock_code,state_code,destination_type;''')
		)
		return div
```

The HTML report is saved in the project directory as `summary.html` with any images for graphs stored in the `summary` sub-directory.
