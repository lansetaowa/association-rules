# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 12:21:24 2024

@author: elisa
"""

# query sales data and product category
receipt_sql = '''
        with tab1 as(
        	select t1.receipt_id,
        				 t1.receipt_timestamp,
        				 t1.user_id
        	from receipts t1
        	left join shops t2
        		on t1.shop_id = t2.shop_id
        	where t1.receipt_timestamp between '{0}' and '{1}'  
        	and t1.receipt_type=0
        	and t2.region_block_code='sh-lawson'
        )
        
        select tab1.receipt_id,
        		   tab1.receipt_timestamp,
        			 tab1.user_id,
        			 tab2.product_id,
        			 tab3.friendly_name,
        			 tab4.small,
        			 tab4.medium,
        			 tab4.large
        from tab1
        left join receipt_item tab2
        on tab1.receipt_id = tab2.receipt_id
        	and tab2.promotion_type = 0
        left join products tab3
        on tab2.product_id = tab3.product_id
        left join product_id_sml_class tab4
        on tab2.product_id = tab4.product_id
        limit 100000
'''
