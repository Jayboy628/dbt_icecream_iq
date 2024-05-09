with

orders as (

    select * from {{ source('retail', 'orders') }}

),

final as (

	SELECT
        orderid,
        COALESCE('HSD|' || customerid, '0') as customerid,
        COALESCE('HSD|' || employeeid, '0') as employeeid,
        COALESCE('HSD|' || deliveryaddressid, '0') as delivery_addressid,
        COALESCE('HSD|' || paymenttypeid, '0') as payment_typeid,
        orderdate as order_date,
        comments,
        status,
        deliverydate AS delivery_date,
        modifieddate as modified_date
    FROM ORDERS
)

select * from final