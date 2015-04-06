'''
A module for importing, and providing a list of, all the checks defined in the other modules
'''
from check import Check
from checks_trip_details import *
from checks_estimated_subcatch import *
from checks_fishing_event import *
from checks_landing import *
from checks_fishing_event_landing import *
from checks_allocation import *

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
	FESAS,
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