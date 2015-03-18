# groomer

<img alt="Gratuitous image of an implement used for grooming" src="http://upload.wikimedia.org/wikipedia/commons/thumb/9/97/Black_Comb.jpg/320px-Black_Comb.jpg" align="middle">

### What and why

A system for requesting, loading, grooming and merging New Zealand fisheries catch and effort data. We use `groomer` at [Trophia](http://www.trophia.com) in analyses of this sort of data.

### History and status

Groomer was initially developed in 2008 to implement the grooming and merging algorithms described in Starr (2007) (and subsequent revisions). In 2011, as part of the "Tanga" family of tools used by Trophia for analyses of catch and effort data, it was dubbed `tangagroomer` (the other tools being `tangaviewer` and `tangaanalyser`) and setup as a [Mercurial](http://mercurial.selenic.com/) repository. It has been used extensively by some Trophia associates (others use R or Stata versions) but has not had any major development since 2012. In March 2015, `tangagroomer` was renamed `groomer`, switched over to a [Git](http://git-scm.com) repository, and open sourced under the [GPLv3 licence](LICENCE.txt).

This software was developed more than six years ago. If we were to write this again we would do things differently. We don't plan on doing a major revamp and we don't necessarily recommend that anyone else use it. Our intension in open sourcing `groomer` is simply to provide others in the NZ fisheries science community with ideas for approaches and techniques - please feel free to take the good bits for your own project and leave the bad. Specifically, we are hoping that there might be something in here that is useful for [Trident's](http://www.tridentsystems.co.nz/) `kahawhai` database and hope to be able to abandon `groomer` in favour of collectively contributing to, and making use of, `kahawai`. 

There is not a lot of documentation other than this README and the documentation strings and comments in the code. If you would like advice on what exactly the "good bits" are, please contact [Nokome](http://trophia.com/team/nokome-bentley/)...or read on. You probably won't get much help if all you want to do it run this thing. Like it says above, that's not why this is here.

Starr, P.J. (2007). Procedure for merging MFish Landing and Effort data. V2.0. Unpublished report held by Ministry for Primary Industries as document AMPWG 07/04. 17 p.

### Overview

Groomer uses Python and SQL with `sqlite` as the database engine. Most of the heavy lifting numerical stuff is done in sqlite with SQL. Python provides the "frontend" API, generates some of the SQL, and generates the HTML summary reports.

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

The `email.txt` file contains draft text for an email to send to RDM. It describes the extract criteria, as specified in the `Dataset` definition in words:

```
Dear RDM,

We have been contracted by New Zealand Ministry of Primary Industries under project "BFF2015-01 : Characterization of big bluefish in BBF 9" to analyse catch and effort data for BFF9. We would like to obtain catch and effort data for fishing trips that occurred between 01 Oct 1989 and 01 Oct 2014, and which landed to BFF9 , or which had fishing events that:
  - were in statistical area(s) 091, 092, 099, and,
  - used method(s) BT, BLL, and,
  - targeted species BBF, BNS, ALB
...
```

The `.sql` files are also generated based on the `Dataset` definition. The most important, and the one which the other SQLs are based, is the `tt.sql` which creates a temporary table which lists the trips that qualify for the dataset:

```
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

When you get the extract back from MPI you extract it into a subdirectory called `extract`.

