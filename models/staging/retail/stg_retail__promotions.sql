
with

promotions as (

    select * from {{ source('retail', 'promotions') }}

),

final as (

	SELECT PromotionID		AS promotionid,
	      DealDescription	AS deal_description,	
	      StartDate			AS start_date,
	      EndDate			AS end_date,
	      DiscountAmount	AS discount_amount,
	      DiscountPercentage AS discount_percentage,
	      ModifiedDate		AS modified_date,
	FROM promotions
)

select * from final