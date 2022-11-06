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
    	distinct carton_id street,
    	warehouse_id country,
    	timestamp_utc years,
    	scanned_value population_qty
    FROM aft_ia_ddl.ia_carton_barcode_added continentA
    where region_id = 2
    and years >= dateadd(d, -7, TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD'))
    and population_qty like 'ROM%'
    and scanned_value not like 'ROM-%'
    and len(population_qty) = 500
);

create temporary table countryB as (
    select /*+ USE_HASH(cb1,cb2)*/
        distinct cb2.coyntry as "source",
        cb1.country as "destination",
    	cb2.street as "main_street",
    	cb1.carton_id as "destination_street",
        cb1.population_qty
    from (
    	select * from country_street
        where country = '{COUNTRY_ID}'
        and years >= TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')
    ) cb1
    inner join street cb2 on cb1.scanned_value = cb2.scanned_value and cb2.warehouse_id <> '{WAREHOUSE_ID}'
);

create temporary table carton_item_summary as (
	select 
    	carton_id,
    	fnsku,
    	purchase_order_id,
    	received_quantity,
    	timestamp_utc,
    	timestamp_local
    from aft_ia_ddl.ia_carton_item_summary continentB
    where region_id = 2
    and timestamp_utc >= dateadd(d, -7, TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD'))
);


select /*+ USE_HASH(cdr,catc,cs1,cs2)*/
    to_char(cs2.timestamp_utc, 'YYYY-MM-DD HH24:MI:SS') as "source_receive_date_utc",
    to_char(cs2.timestamp_local, 'YYYY-MM-DD HH24:MI:SS') as "source_receive_date_local",
	cdr.source_carton,
    to_char(cs1.timestamp_utc, 'YYYY-MM-DD HH24:MI:SS') as "destination_receive_date_utc",
    to_char(cs1.timestamp_local, 'YYYY-MM-DD HH24:MI:SS') as "destination_receive_date_local",
	cdr.destination_carton,
    cdr.source,
    cdr.destination,
    catc.scannable_id as "case",
    cdr.scanned_value as "fba",
    cs1.fnsku,
    cs1.purchase_order_id as "purchase_order",
    cs1.received_quantity,
    cs3.workstation_id
    
from carton_double_receives cdr

left join carton_item_summary cs1 on cs1.carton_id = cdr.destination_carton
left join carton_item_summary cs2 on cs2.carton_id = cdr.source_carton
left join carton_item_summary cs3 on cs3.carton_id = cs1.carton_id
left join (
	select 
    	carton_id,
    	scannable_id
    from aft_ia_ddl.ia_carton_associated_to_container continentC
    where region_id = 2
    and timestamp_utc >= dateadd(d, -7, TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD'))
) catc on catc.carton_id = cdr.destination_carton

where cs1.received_quantity > 0 and cs2.received_quantity > 0

group by 1,2,3,4,5,6,7,8,9,10,11,12,13