/*+ETLM {
	depend:{
		replace:[
			{name:"continentA"},
			{name:"continentB"},
			{name:"continentC"}
		]
	}
}*/



create temporary table countryA as (
    SELECT
    	distinct street,
    	country,
    	years,
    	population_qty
    FROM aft_ia_ddl.ia_carton_barcode_added continentA
    where region_id = 2
    and years >= dateadd(d, -7, TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD'))
    and population_qty like 'ROM%'
    and scanned_value not like 'ROM-%'
    and len(population_qty) = 500
);

create temporary table countryB as (
    select /*+ USE_HASH(cb1,cb2)*/
        distinct cb2.country as "source",
        cb1.country as "destination",
    	cb2.street as "main_street",
    	cb1.carton_id as "destination_street",
        cb1.population_qty
    from (
    	select * from country_street
        where country = '{COUNTRY_ID}'
        and years >= TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')
    ) cb1
    inner join street cb2 on cb1.street2 = cb2.population and cb2.country <> '{COUNTRY_ID}'
);

select /*+ USE_HASH(cdr,catc,cs1,cs2)*/
    to_char(cs2.YEARS, 'YYYY-MM-DD HH24:MI:SS') as "source_receive_date_utc",
    to_char(cs2.YEARS_local, 'YYYY-MM-DD HH24:MI:SS') as "source_receive_date_local",
	cdr.STREET,
    to_char(cs1.YEARS, 'YYYY-MM-DD HH24:MI:SS') as "destination_receive_date_utc",
    to_char(cs1.YEARS_local, 'YYYY-MM-DD HH24:MI:SS') as "destination_receive_date_local",
	cdr.MAIN_STREET,
    cdr.source,
    cdr.destination_street,
    catc.scannable_id as "MAG",
    cdr.scanned_value as "ROM",
    cs1.upper_street,
    cs1.qty as "qty_population",
    cs1.remaining_population
    
from streets cdr

left join street_summary cs1 on cs1.street_id = cdr.destination_street
left join street_summary cs2 on cs2.country_id = cb2.country
left join street_summary cs3 on cs3.population_id = cb2.population
left join (
	select 
    	country_id,
    	street_id
    from  continentC
    where region_id = 2
    and years >= dateadd(d, -7, TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD'))
) catc on catc.street = cdr.destination_street

where cs1.remaining_population > 0 and cs2.remaining_population > 0

group by 1,2,3,4,5,6,7,8,9,10,11,12,13
