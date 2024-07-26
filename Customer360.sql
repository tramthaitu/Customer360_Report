create temp table rfm_calculation as (
select R.id as customer_id,
'2022-09-01'- max(purchase_date) as recency,
round(count(distinct(purchase_date))/(2022-extract(year from created_date)),2) as frequency ,
round(sum(GMV)/(2022-extract(year from created_date)),2) as monetary,
dense_rank() over (order by '2022-09-01' - max(purchase_date)) as r_recency,
dense_rank() over (order by round(count(distinct(purchase_date))/(2022-extract(year from created_date)),2)) as r_frequency,
dense_rank() over (order by round(sum(GMV)/(2022-extract(year from created_date)),2)) as r_monetary
from customer_transaction T
join customer_register R 
on T.customerid=R.id and stopdate is null
where customerid != 0 
group by r.id , created_date)

with result as (
select customer_id,recency,frequency,monetary,r_recency,r_frequency,r_monetary,
case 
	when r_recency >= min(r_recency) over () 
		 and r_recency < (select r_recency from rfm_calculation order by r_recency limit 1 offset (select (count(customer_id) * 0.25)::int from rfm_calculation))
		then '1'
	when r_recency >= (select r_recency from rfm_calculation order by r_recency limit 1 offset (select (count(customer_id) * 0.25)::int from rfm_calculation))
		 and r_recency < (select r_recency from rfm_calculation order by r_recency limit 1 offset (select (count(customer_id) * 0.5)::int from rfm_calculation))
		then '2'
	when r_recency >= (select r_recency from rfm_calculation order by r_recency limit 1 offset (select (count(customer_id) * 0.5)::int from rfm_calculation))
		 and r_recency < (select r_recency from rfm_calculation order by r_recency limit 1 offset (select (count(customer_id) * 0.75)::int from rfm_calculation))
		then '3'
	else '4'
end as r,
case 
	when r_frequency >= min(r_frequency) over () 
		 and r_frequency < (select r_frequency from rfm_calculation order by r_frequency limit 1 offset (select (count(customer_id) * 0.25)::int from rfm_calculation))
		then '4'
	when r_frequency >= (select r_frequency from rfm_calculation order by r_frequency limit 1 offset (select (count(customer_id) * 0.25)::int from rfm_calculation))
		 and r_frequency < (select r_frequency from rfm_calculation order by r_frequency limit 1 offset (select (count(customer_id) * 0.5)::int from rfm_calculation))
		then '3'
	when r_frequency >= (select r_frequency from rfm_calculation order by r_frequency limit 1 offset (select (count(customer_id) * 0.5)::int from rfm_calculation))
		 and r_frequency < (select r_frequency from rfm_calculation order by r_frequency limit 1 offset (select (count(customer_id) * 0.75)::int from rfm_calculation))
		then '2'
	else '1'
end as f,
case 
	when r_monetary >= min(r_monetary) over () 
		 and r_monetary < (select r_monetary from rfm_calculation order by r_monetary limit 1 offset (select (count(customer_id) * 0.25)::int from rfm_calculation))
		then '4'
	when r_monetary >= (select r_monetary from rfm_calculation order by r_monetary limit 1 offset (select (count(customer_id) * 0.25)::int from rfm_calculation))
		 and r_monetary < (select r_monetary from rfm_calculation order by r_monetary limit 1 offset (select (count(customer_id) * 0.5)::int from rfm_calculation))
		then '3'
	when r_monetary >= (select r_monetary from rfm_calculation order by r_monetary limit 1 offset (select (count(customer_id) * 0.5)::int from rfm_calculation))
		 and r_monetary < (select r_monetary from rfm_calculation order by r_monetary limit 1 offset (select (count(customer_id) * 0.75)::int from rfm_calculation))
		then '2'
	else '1'
end as m
from rfm_calculation
group by customer_id,recency,frequency,monetary,r_recency,r_frequency,r_monetary
order by r asc, f asc, m asc),
cte as (
select *, 
(r || f || m)::integer AS RFM,
case
	when (r || f || m)::integer in (111, 211, 212, 221, 222) then 'Champions'
	when (r || f || m)::integer in (322, 321, 312, 311, 422,421, 412, 411) then 'Loyalists'
	when (r || f || m)::integer in (134, 133, 132, 131, 144,143, 142, 141, 244, 243,242, 241, 234, 233, 232,231) then 'Potential'
	else 'Walk-in Customers'
end as segmentation
from result) 

select *
from cte 
