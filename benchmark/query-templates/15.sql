-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Variant A
-- Approved February 1998
--
-- BemiDB: Variant without "view"
:x
  with revenue (supplier_no, total_revenue) as (
    select
      l_suppkey,
      sum(l_extendedprice * (1-l_discount))
    from
      lineitem
    where
      l_shipdate >= date ':1'
      and l_shipdate < date ':1' + interval '3 months'
    group by
      l_suppkey
  )

  :o
  select
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
  from
  supplier,
  revenue
  where
  s_suppkey = supplier_no
  and total_revenue = (
    select
      max(total_revenue)
    from
      revenue
  )
  order by
  s_suppkey;
  :n -1
