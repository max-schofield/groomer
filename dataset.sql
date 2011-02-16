/*
Create a tanga database to hold all the data from a tanga extract.
*/

DROP TABLE IF EXISTS status;
CREATE TABLE status (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	task TEXT,
	done DATETIME
);

DROP TABLE IF EXISTS trip_details;
CREATE TABLE trip_details (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	trip INTEGER,
	trip_version INTEGER,
	start_datetime DATETIME,
	end_datetime DATETIME,
	vessel_key INTEGER,
	client_key INTEGER
);
CREATE INDEX trip_details_trip ON trip_details(trip);


DROP TABLE IF EXISTS fishing_event;
CREATE TABLE fishing_event (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_key INTEGER,
	version_seqno INTEGER,
	group_key INTEGER,
	start_datetime DATETIME,
	end_datetime DATETIME,
	primary_method TEXT,
	target_species TEXT,
	fishing_duration REAL,
	catch_weight REAL,
	catch_weight_other REAL,
	non_fish_yn BOOLEAN,
	effort_depth REAL,
	effort_height REAL,
	effort_num INTEGER,
	effort_num_2 INTEGER,
	effort_seqno INTEGER,
	effort_total_num INTEGER,
	effort_width REAL,
	effort_length REAL,
	effort_speed REAL,
	surface_temp REAL,
	total_hook_num INTEGER,
	set_end_datetime DATETIME,
	haul_start_datetime DATETIME,
	haul_start_wind_speed REAL,
	haul_end_wind_speed REAL,
	set_start_wind_speed REAL,
	set_start_wind_direction TEXT,
	haul_end_wind_direction TEXT,
	haul_end_surface_temp REAL,
	float_num INTEGER,
	light_stick_num INTEGER,
	line_shooter_yn BOOLEAN,
	condition_type TEXT,
	total_net_length REAL,
	double_reel_num INTEGER,
	pair_trawl_yn BOOLEAN,
	bottom_depth REAL,
	trunc_start_lat REAL,
	trunc_start_long REAL,
	trunc_end_lat REAL,
	trunc_end_long REAL,
	start_stats_area_code TEXT,
	vessel_key INTEGER,
	client_key INTEGER,
	dcf_key  INTEGER,
	form_type TEXT,
	trip INTEGER
);
CREATE INDEX fishing_event_event_key_version_seqno ON fishing_event(event_key,version_seqno);
CREATE INDEX fishing_event_event_key ON fishing_event(event_key);
CREATE INDEX fishing_event_vessel_key ON fishing_event(vessel_key);
CREATE INDEX fishing_event_trip ON fishing_event(trip);
CREATE INDEX fishing_event_start_stats_area_code ON fishing_event(start_stats_area_code);
CREATE INDEX fishing_event_trip_start_stats_area_code ON fishing_event(trip,start_stats_area_code);
CREATE INDEX fishing_event_primary_method ON fishing_event(primary_method);
CREATE INDEX fishing_event_target_species ON fishing_event(target_species);
CREATE INDEX fishing_event_form_type ON fishing_event(form_type);

DROP TABLE IF EXISTS estimated_subcatch;
CREATE TABLE estimated_subcatch (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_key INTEGER,
	version_seqno INTEGER,
	group_key INTEGER,
	species_code TEXT,
	catch_weight REAL
);
CREATE INDEX estimated_subcatch_event_key_version_seqno ON estimated_subcatch(event_key,version_seqno);
CREATE INDEX estimated_subcatch_event_key ON estimated_subcatch(event_key);
CREATE INDEX estimated_subcatch_species_code ON estimated_subcatch(species_code);

DROP TABLE IF EXISTS processed_catch;
CREATE TABLE processed_catch (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_key INTEGER,
	version_seqno INTEGER,
	group_key INTEGER,
	specprod_seqno INTEGER,
	specprod_action_type TEXT,
	processed_datetime DATETIME,
	species_code TEXT,
	state_code TEXT,
	unit_type TEXT,
	unit_num INTEGER,
	unit_weight REAL,
	conv_factor REAL,
	processed_weight REAL,
	processed_weight_type TEXT,
	green_weight REAL,
	green_weight_type TEXT,
	dcf_key INTEGER,
	form_type TEXT,
	trip INTEGER
);
CREATE INDEX processed_catch_event_key_version_seqno ON processed_catch(event_key,version_seqno);
CREATE INDEX processed_catch_trip ON processed_catch(trip);

DROP TABLE IF EXISTS landing;
CREATE TABLE landing (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_key INTEGER,
	version_seqno INTEGER,
	group_key INTEGER,
	specprod_seqno INTEGER,
	landing_datetime DATETIME,
	landing_name TEXT,
	species_code TEXT,
	fishstock_code TEXT,
	state_code TEXT,
	destination_type TEXT,
	unit_type TEXT,
	unit_num INTEGER,
	unit_num_latest INTEGER,
	unit_num_other INTEGER,
	unit_weight REAL,
	conv_factor REAL,
	green_weight REAL,
	green_weight_type TEXT,
	processed_weight REAL,
	processed_weight_type TEXT,
	tranship_vessel_key INTEGER,
	vessel_key INTEGER,
	client_key INTEGER,
	dcf_key INTEGER,
	form_type TEXT,
	trip INTEGER
);
CREATE INDEX landing_event_key_version_seqno ON landing(event_key,version_seqno);
CREATE INDEX landing_trip ON landing(trip);
CREATE INDEX landing_vessel_key ON landing(vessel_key);
CREATE INDEX landing_species_code ON landing(species_code);
CREATE INDEX landing_fishstock_code ON landing(fishstock_code);
	

DROP TABLE IF EXISTS vessel_history;
CREATE TABLE vessel_history (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	history_start_datetime DATETIME,
	history_end_datetime DATETIME,
	vessel_key INTEGER,
	flag_nationality_code TEXT,
	overall_length_metres REAL,
	registered_length_metres REAL,
	draught_metres REAL,
	beam_metres REAL,
	gross_tonnes REAL,
	max_speed_knots REAL,
	service_speed_knots REAL,
	engine_kilowatts REAL,
	max_duration_days REAL,
	built_year INTEGER,
	tenders_number INTEGER,
	total_crew_number INTEGER,
	base_region_code TEXT,
	base_port_code TEXT
);
CREATE INDEX vessel_history_vessel_key ON vessel_history(vessel_key);

DROP TABLE IF EXISTS mhr;
CREATE TABLE mhr (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	month INTEGER,
	year INTEGER,
	stock_code TEXT,
	quantity REAL,
	perorg_key INTEGER
);
CREATE INDEX mhr_year_month ON mhr(year,month);
CREATE INDEX mhr_stock_code ON mhr(stock_code);
CREATE INDEX mhr_year_month_stock_code ON mhr(year,month,stock_code);
CREATE INDEX mhr_perorg_key ON mhr(perorg_key);

DROP TABLE IF EXISTS qmr;
CREATE TABLE qmr (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	year INTEGER,
	month INTEGER,
	fishstock TEXT,
	client_key INTEGER,
	quantity REAL
);
CREATE INDEX qmr_year_month ON qmr(year,month);
CREATE INDEX qmr_fishstock ON qmr(fishstock);
CREATE INDEX qmr_year_month_fishstock ON qmr(year,month,fishstock);
CREATE INDEX qmr_client_key ON qmr(client_key);

DROP TABLE IF EXISTS history;
CREATE TABLE history (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	fishing_year INTEGER,
	fishstock TEXT,
	landings REAL,
	TAC REAL,
	TACC REAL
);
CREATE INDEX history_fishing_year_fishstock ON history(fishing_year,fishstock);

DROP TABLE IF EXISTS dqss;
CREATE TABLE dqss (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	species TEXT,
	method TEXT,
	proc_c REAL,
	proc_a REAL,
	units TEXT
);
CREATE INDEX dqss_species_method ON dqss(species,method);

DROP TABLE IF EXISTS qmastats;
CREATE TABLE qmastats (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	species TEXT,
	qma TEXT,
	stat TEXT
);
CREATE INDEX qmastats_qma ON qmastats(qma);

