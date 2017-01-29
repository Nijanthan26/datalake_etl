drop table if exists antuit_pricing.ext_customer_xref;
Create external table antuit_pricing.ext_customer_xref
(
id string
,isdeleted string
,name							string
,currencyisocode				string
,createddate					timestamp
,createdbyid					string
,lastmodifieddate				timestamp
,lastmodifiedbyid				string
,systemmodstamp					timestamp
,lastvieweddate					timestamp
,lastreferenceddate				timestamp
,lin_account__c					string
,lin_customer_enterprise_id__c	string
,lin_legacy_customer_code__c	string
,lin_legacy_customer_name__c	string
,lin_payment_terms__c			string
,lin_source_system_id__c		string
,lin_source_system_name__c		string
,lin_survivor_customer_name__c	string
,lin_workday_location_id__c		string
,lin_workday_location_name__c	string
,paybypo__c						string
,intercompany__c				string
,effective_start_date__c		timestamp
,effective_end_date__c			timestamp
,ic_wd_loc__c					string
,ic_wd_cc__c					string
)
row format delimited fields terminated by '|'
LOCATION '/antuit/xref/XREF_Customer'
tblproperties("skip.header.line.count"="1")
;


drop table if exists antuit_pricing.ext_chargecode_xref;
Create external table antuit_pricing.ext_chargecode_xref
(
id	string
,ownerid	string
,isdeleted	string
,name	string
,currencyisocode	string
,createddate	timestamp
,createdbyid	string
,lastmodifieddate	timestamp
,lastmodifiedbyid	string
,systemmodstamp	timestamp
,lastvieweddate	timestamp
,lastreferenceddate	timestamp
,lin_consolidated_category__c	string
,lin_consolidated_charge_code_pct__c	string
,lin_consolidated_charge_code__c	string
,lin_consolidated_charge_name__c	string
,lin_consolidated_gl_account__c	string
,lin_is_hpp__c	string
,lin_legacy_charge_code__c	string
,lin_legacy_charge_name__c	string
,lin_legacy_system__c	string
,effective_start_date__c	timestamp
,effective_end_date__c	timestamp
,legacy_charge_code_description__c	string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/antuit/xref/XREF_ChargeCode'
tblproperties("skip.header.line.count"="1")
;


drop table if exists antuit_pricing.ext_costcenter_xref;
create external table antuit_pricing.ext_costcenter_xref
(
id	string
,ownerid	string
,isdeleted	string
,name	string
,currencyisocode	string
,createddate	timestamp
,createdbyid	string
,lastmodifieddate	timestamp
,lastmodifiedbyid	string
,systemmodstamp	timestamp
,lastvieweddate	timestamp
,lastreferenceddate	timestamp
,legacy_lin_company_id__c	string
,legacy_lin_department__c	string
,legacy_lin_location__c	string
,lin_ship_to_work_id__c	string
,legacy_lin_system__c	string
,lin_workday_cost_center__c	string
,lin_workday_location_id__c	string
,workday_company_id__c	string
,fgl_code__c	string
,effective_start_date__c	timestamp
,effective_end_date__c	timestamp
,lin_workday_location_name__c	string
,warehouse_location__c	string
)
row format delimited fields terminated by '|'
location '/antuit/xref/XREF_CostCenter'
tblproperties("skip.header.line.count"="1")
;



/*
drop table if exists antuit_pricing.ext_account_revised_xref;
Create external table antuit_pricing.ext_account_revised_xref
(
ID string,
NAME string,
BILLINGSTREET string,
BILLINGCITY string,
BILLINGSTATE string,
BILLINGPOSTALCODE string,
BILLINGCOUNTRY string,
PHONE string,
FAX string,
ACCOUNTNUMBER string,
WEBSITE string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/antuit/xref/XREF_Account_Revised/'
tblproperties("skip.header.line.count"="1")
;
*/

drop table if exists antuit_pricing.ext_account_revised_xref;
create external table antuit_pricing.ext_account_revised_xref
(
id string
,name string
,recordtypeid string
,parentid string
,billingstreet string
,billingcity string
,billingstate string
,billingpostalcode string
,billingcountry string
,billinglatitude string
,billinglongitude string
,billinggeocodeaccuracy string
,billingaddress string
,shippingstreet string
,shippingcity string
,shippingstate string
,shippingpostalcode string
,shippingcountry string
,shippinglatitude string
,shippinglongitude string
,shippinggeocodeaccuracy string
,shippingaddress string
,phone string
,fax string
,accountnumber string
,website string
,photourl string
,sic string
,industry string
,annualrevenue string
,numberofemployees string
,ownership string
,tickersymbol string
,description string
,rating string
,site string
,currencyisocode string
,ownerid string
,createddate string
,createdbyid string
,lastmodifieddate string
,lastmodifiedbyid string
,systemmodstamp string
,lastactivitydate string
,lastvieweddate string
,lastreferenceddate string
,jigsaw string
,jigsawcompanyid string
,cleanstatus string
,accountsource string
,dunsnumber string
,tradestyle string
,naicscode string
,naicsdesc string
,yearstarted string
,sicdesc string
,dandbcompanyid string
,data_quality_description__c string
,data_quality_score__c string
,toll_free_phone__c string
,cis_account_number__c string
,revenue_activity__c string
,status__c string
,customer_status_assigned__c string
,customer_status__c string
,xref_table_updated1_c__c string
,accpac_customer_number__c string
,access_via_intersection__c string
,air_defrost_available__c string
,approved_us_importing__c string
,approved_for_exporting__c string
,barrel_clamp_services_available__c string
,born_on_date__c string
,certified_for_reinspection__c string
,chicago_merchantile_approved__c string
,cubic_feet__c string
,publishtoworkday__c string
,notpublishedinworkday__c string
,drop_trailer_parking_spots__c string
,freight_consolidation__c string
,general_manager__c string
,grandparent_account__c string
,haccp_certified_facility__c string
,handle_ice_cream_15f__c string
,if_approved_usda__c string
,inbond_available__c string
,land_available_for_build_to_suit__c string
,legacyid__c string
,location_code__c string
,mas200_customer_number__c string
,mas500_customer_number__c string
,multi_temp_or_frozen__c string
,number_of_blast_cells__c string
,number_of_rail_doors__c string
,number_of_truck_doors__c string
,onboarding_segmentation__c string
,phase_ii_onboarding_opt_out__c string
,process_space_available__c string
,product_boxing_available__c string
,rdc_or_dc__c string
,rail_provider_1__c string
,rail_provider_2__c string
,receiving_hours_of_operation__c string
,refrigerated_docks__c string
,region__c string
,shipping_hours_of_operation__c string
,slip_sheet_services_available__c string
,speed_dial__c string
,squeeze_pack_services_available__c string
,temperature_range__c string
,total_square_feet__c string
,usda_approved__c string
,us_bioterrorism__c string
,workday_account__c string
,walmart_sam_s_consolidation_program__c string
,water_defrost_available__c string
,of_rail_doors_for_50_ft_cars_day__c string
,of_rail_doors_for_super_car_day__c string
,accpac_national_acct__c string
,payment_terms__c string
,remit_from__c string
,customer_category__c string
,customer_group__c string
,mrs__c string
,hj_warehouse_id__c string
,aec__c string
,loop__c string
,worcs__c string
,dc_wizard__c string
,inforexceed__c string
,inforsce__c string
,synapse_cloud__c string
,rate_letter_contact__c string
,zethcon__c string
,kostecia_system__c string
,accellosse2__c string
,andlor__c string
,infor__c string
,foursite__c string
,lean__c string
,x3plogic__c string
,manufacturing__c string
,redistribution__c string
,hj_customer_code__c string
,enterprise_id__c string
,mh_additional_notes_exist__c string
,mh_associated_gold_sheet__c string
,mh_charter_statement__c string
,mh_critical_vulnerability__c string
,mh_fop_last_yr__c string
,mh_fop_opportunities__c string
,mh_fop_trends__c string
,mh_fop_view_1_yr__c string
,mh_fop_view_3_yr__c string
,mh_fop_view_now__c string
,mh_goals__c string
,mh_last_updated_gold_sheet__c string
,mh_managers_notes_exist__c string
,mh_managers_review_date__c string
,mh_our_view_1_yr__c string
,mh_our_view_3_yr__c string
,mh_our_view_last_yr__c string
,mh_our_view_now__c string
,mh_gold_sheet_created_date__c string
,mh_strengths__c string
,term_start_date__c string
,term_expiration_date__c string
,cust_id_tidemark__c string
,accpac_national_account__c string
,use_for_gold_sheet_review__c string
,highjump2__c string
,merge_mas500__c string
,data_source__c string
,erp_name__c string
,lin_credit_limit_currency__c string
,lin_credit_limit_verification_date__c string
,lin_credit_limit__c string
,lin_parent_enterprise_id__c string
,master_record_nbr__c string
,merge_status__c string
,remitfrom_enterpriseid__c string
,lin_no_of_xref__c string
,remitfromused_status__c string
,ultimate_parent__c string
,wdintegrate__c string
,xref_table_updated__c string
,submunicipality__c string
,lost_customer_facilities__c string
,lost_customer_facility_comments__c string
,lost_customer_facilitycategory__c string
,lost_customer_facility_reason__c string
,updatewdintegratedatetime__c string
,update__c string
,phone_account_table_updated__c string
,remit_alternate_name__c string
,rollup_ar_phone__c string
,phone_account_table_updated1__c string
,accellos_gp__c string
,request_to_create_rl__c string
)
row format delimited fields terminated by '\t'
location '/antuit/xref/XREF_Account_Revised/'
tblproperties("skip.header.line.count"="1")
;


drop table antuit_pricing.customer_xref;
create table antuit_pricing.customer_xref as 
select 
x.*,
concat(
nvl(cast(x.lin_source_system_name__c as string),cast('' as string)),
nvl(cast(x.lin_source_system_id__c as string),cast('' as string)),
nvl(cast(x.lin_legacy_customer_code__c as string),cast('' as string))) as uid
from antuit_pricing.ext_customer_xref x;



drop table antuit_pricing.chargecode_xref;
create table antuit_pricing.chargecode_xref as 
select 
x.*,
concat(
nvl(cast(x.lin_legacy_system__c as string),cast('' as string)),
nvl(cast(x.lin_legacy_charge_code__c as string),cast('' as string))) as uid
from antuit_pricing.ext_chargecode_xref x;



drop table antuit_pricing.costcenter_xref;
create table antuit_pricing.costcenter_xref as 
select 
x.*,
concat(
nvl(cast(x.legacy_lin_system__c as string),cast('' as string)),
nvl(cast(x.legacy_lin_location__c as string),cast('' as string))) as uid
from antuit_pricing.ext_costcenter_xref x;



drop table antuit_pricing.account_revised_xref;
create table antuit_pricing.account_revised_xref as 
select 
distinct
id,
name,
billingstreet,
billingcity,
billingstate,
billingpostalcode,
billingcountry,
phone,
fax,
accountnumber,
website 
from antuit_pricing.ext_account_revised_xref;