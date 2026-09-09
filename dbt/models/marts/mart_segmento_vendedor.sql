
select d.year_month, 
       s.seller_segment, sum(o.price) as gmv,
       count(o.order_item_id) as items_vendidos,
       count(distinct o.order_id) as ordenes
       from {{source('gold', 'fct_order_items')}} as o
inner join {{source('gold', 'dim_seller')}} s on o.seller_sk = s.seller_sk
inner join {{source('gold', 'dim_date')}} d on o.date_sk = d.date_sk
where d.date_sk != -1
group by d.year_month, s.seller_segment
