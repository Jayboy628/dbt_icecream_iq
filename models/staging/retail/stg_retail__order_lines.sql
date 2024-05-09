
with

order_lines as (

    select * from {{ source('retail', 'orderlines') }}

),

final as (

	SELECT OrderLineID 			AS order_lineid
	      ,OrderID				AS orderid
	      ,COALESCE('HSD|' || ProductID, '0')			AS productid
	      ,PackageTypeID		AS package_typeid
	      ,COALESCE('HSD|' ||PromotionID, '0')			AS promotionid
	      ,InventoryItemID		AS inventory_itemid
	      ,UnitPrice			AS unit_price
	      ,Description			AS description
	      ,Quantity 			AS quantity
	      ,Discount				AS discount
	      ,ModifiedDate			AS modified_date
	      ,LineNumber			AS line_number
	      ,VATRate				AS vat_rate
	  FROM order_lines
)

select * from final