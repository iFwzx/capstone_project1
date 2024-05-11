CREATE OR REPLACE DATABASE TPCDS;

CREATE OR REPLACE SCHEMA RAW;

CREATE OR REPLACE TABLE TPCDS.RAW.INVENTORY (

inv_data_sk int not null,
inv_item_sk int not null,
inv_quantity_on_hand int,
inv_warehouse_sk int not null
);