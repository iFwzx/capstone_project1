-- null test
-- customer_ id is not null
select count(*) = 0 from tpcds.analytics.customer_dim
where c_customer_sk is null;

-- unique test
-- wharehouse_sk, item_ and sold_wk_sk is unique
select count(*) = 0 from
( select
warehouse_sk, item_sk, sold_wk_sk
from tpcds.analytics.weekly_sales_inventory
group by 1,2,3
having count(*)>1 );

-- relationship test 
select count(*) = 0 from
(select 
dim.i_item_sk
from tpcds.analytics.weekly_sales_inventory fact
left join tpcds.analytics.item_dim dim
on dim.i_item_sk=fact.item_sk
where dim.i_item_sk is null);

-- accepted value test
select count(*) = 0 from tpcds.analytics.weekly_sales_inventory
where warehouse_sk not in (1,2,3,4,5,6);

-- adhoc test
select count(*) = 0 from
(select c_current_cdemo_sk, cd.cd_demo_sk
from tpcds.raw.customer c
left join tpcds.raw.customer_demographics cd
on c.c_current_cdemo_sk = cd.cd_demo_sk
where c_current_cdemo_sk is not null and cd.cd_demo_sk is null);


