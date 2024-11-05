CREATE INDEX IF NOT EXISTS idx_part_name ON part (p_name varchar_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_part_brand_container ON part (p_brand, p_container, p_partkey);
CREATE INDEX IF NOT EXISTS idx_part_partkey ON part (p_partkey);

CREATE INDEX IF NOT EXISTS idx_partsupp_part_supp ON partsupp (ps_partkey, ps_suppkey);
CREATE INDEX IF NOT EXISTS idx_partsupp_suppkey ON partsupp (ps_suppkey);

CREATE INDEX IF NOT EXISTS idx_lineitem_dates ON lineitem (l_shipdate);
CREATE INDEX IF NOT EXISTS idx_lineitem_part_supp ON lineitem (l_partkey, l_suppkey);
CREATE INDEX IF NOT EXISTS idx_lineitem_part_qty ON lineitem (l_partkey, l_quantity, l_extendedprice);

CREATE INDEX IF NOT EXISTS idx_nation_name ON nation (n_name);
CREATE INDEX IF NOT EXISTS idx_nation_nationkey ON nation (n_nationkey);

CREATE INDEX IF NOT EXISTS idx_supplier_nationkey ON supplier (s_nationkey);
