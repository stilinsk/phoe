# phoe
```
version: 2

sources:
  - name: adv
    schema: Raw 
    tables:
      - name: salesorderdetail
        identifier: salesorderdetail
      - name: customer
        identifier: customer
      - name: product
        identifier: product  
      - name: salesorderheader
        identifier: salesorderheader
      - name: productmodel
        identifier: productmodel
      - name: address
        identifier: address

```
bronze
```
With raw_address as (
    select
        *
    from {{ source('adv','address') }}
)
select
   *

from raw_address
```
```
With raw_customers as (
    select
        *
    from {{ source('adv','customer') }}
)
select
   *

from raw_customers
```
```
With raw_productmodel as (
    select
        *
    from {{ source('adv','productmodel') }}
)
select
   *

from raw_productmodel

```
```
With raw_product as (
    select
        *
    from {{ source('adv','product') }}
)
select
   *

from raw_product
```
```
With raw_salesorderdetail as (
    select
        *
    from {{ source('adv','salesorderdetail') }}
)
select
   *

from raw_salesorderdetail
```
```
With raw_salesorderheader as (
    select
        *
    from {{ source('adv','salesorderheader') }}
)
select
   *

from raw_salesorderheader
```
snapshots
```
{% snapshot address_snapshot %}

{{
    config(
        target_schema='DEV',
      
        unique_key='AddressID',
        strategy='timestamp',
        updated_at='ModifiedDate'
    )
}}

select
    AddressID,
    AddressLine1 as AddressLine,
    City,
    StateProvinceID,
    PostalCode,
 modifieddate
from {{ ref('address') }}
where AddressID is not null

{% endsnapshot %}

```
```
{% snapshot customers_snapshot %}

{{
    config(
        target_schema='DEV',
       
        unique_key='CustomerID',
        strategy='timestamp',
        updated_at='ModifiedDate'
    )
}}

select
    CustomerID,
     AddressID,
    StoreID,
    TerritoryID,
    AccountNumber,
   modifieddate
from {{ ref('customers') }}

{% endsnapshot %}

```
```
{% snapshot product_snapshot %}

{{
    config(
        target_schema='DEV',
        unique_key='ProductID',
        strategy='timestamp',
        updated_at='ModifiedDate'
    )
}}

with base as (
    select
        ProductID,
        Name as ProductName,
        ProductNumber,
        MakeFlag,
        FinishedGoodsFlag,
        Color,
        SafetyStockLevel,
        ReorderPoint,
        StandardCost,
        ListPrice,
        Size as ProductSize,
        SizeUnitMeasureCode,
        WeightUnitMeasureCode,
        Weight,
        DaysToManufacture,
        ProductLine,
        Class as ProductClass,
        Style as ProductStyle,
        ProductSubcategoryID,
        ProductModelID,
        SellStartDate,
        SellEndDate,
        ModifiedDate
    from {{ ref('product') }}
    where ProductModelID is not null
),

-- Calculate mode for size unit
size_mode as (
    select SizeUnitMeasureCode
    from base
    where SizeUnitMeasureCode is not null
    group by SizeUnitMeasureCode
    order by count(*) desc
    limit 1
),

-- Calculate mode for weight unit
weight_mode as (
    select WeightUnitMeasureCode
    from base
    where WeightUnitMeasureCode is not null
    group by WeightUnitMeasureCode
    order by count(*) desc
    limit 1
),

weight_avg as (
    select avg(Weight) as avg_weight
    from base
    where Weight is not null
),

aggregates as (
    select
        (select SizeUnitMeasureCode from size_mode) as mode_sizeunit,
        (select WeightUnitMeasureCode from weight_mode) as mode_weightunit,
        (select avg_weight from weight_avg) as avg_weight
),

cleaned as (
    select
        b.ProductID,
        b.ProductName,
        b.ProductNumber,
        b.MakeFlag,
        b.FinishedGoodsFlag,
        b.Color,
        b.SafetyStockLevel,
        b.ReorderPoint,
        b.StandardCost,
        b.ListPrice,
        b.ProductSize,
        coalesce(b.SizeUnitMeasureCode, a.mode_sizeunit) as SizeUnitMeasureCode,
        coalesce(b.WeightUnitMeasureCode, a.mode_weightunit) as WeightUnitMeasureCode,
        coalesce(b.Weight, a.avg_weight) as Weight,
        case when b.DaysToManufacture = 0 then 1 else b.DaysToManufacture end as DaysToManufacture,
        b.ProductLine,
        b.ProductClass,
        b.ProductStyle,
        b.ProductSubcategoryID,
        b.ProductModelID,
        b.SellStartDate,
        b.SellEndDate,
        b.ModifiedDate
    from base b
    cross join aggregates a
)

select * from cleaned

{% endsnapshot %}
```
```
{% snapshot productmodel_snapshot %}

{{
    config(
        target_schema='DEV',
    
        unique_key='ProductModelID',
        strategy='timestamp',
        updated_at='ModifiedDate'
    )
}}

select
    ProductModelID,
    Name as ModelName,
modifieddate
from {{ ref('product_model') }}
where ProductModelID is not null

{% endsnapshot %}
```
```
{% snapshot salesorderdetail_snapshot %}

{{
    config(
        target_schema='DEV',
       
        unique_key='SalesOrderDetailID',
         strategy='timestamp',
        updated_at='ModifiedDate'
    )
}}

select
    SalesOrderID,
    SalesOrderDetailID,
    CarrierTrackingNumber,
    OrderQty as OrderQuantity,
    ProductID,
    SpecialOfferID,
    UnitPrice,
    UnitPriceDiscount,
    round(LineTotal, 0) as TotalPrice,
    ModifiedDate
from {{ ref('salesorderdetail') }}
WHERE SalesOrderID is not null

{% endsnapshot %}

```
```
{% snapshot salesorderheader_snapshot %}

{{
    config(
        target_schema='DEV',
        unique_key='SalesOrderID',
        strategy='timestamp',
        updated_at='ModifiedDate'
    )
}}

select
    SalesOrderID,
    RevisionNumber,
    cast(OrderDate as date) as OrderDate,
    cast(DueDate as date) as DueDate,
    cast(ShipDate as date) as ShipDate,
    datediff(day, OrderDate, DueDate) as DaysToSaleDue,
    datediff(day, OrderDate, ShipDate) as DaysToSalesShip,
    Status,
    OnlineOrderFlag,
    SalesOrderNumber,
    PurchaseOrderNumber,
    AccountNumber,
    CustomerID,
    SalesPersonID,
    TerritoryID,
    BillToAddressID,
    ShipToAddressID,
    ShipMethodID,
    CreditCardID,
    CreditCardApprovalCode,
    CurrencyRateID,
    SubTotal,
    TaxAmt,
    Freight,
    TotalDue,
  ModifiedDate
from {{ ref('salesorderheader') }}
where SalesOrderID is not null

{% endsnapshot %}
```
gold
```
{{
    config(
        materialized ='incremental',
        on_schema_change ='fail'
    )
}}

-- Get current valid customers frmo  the already created silver layer data 
with customers as (
    select
        CustomerID,
        AddressID,
        StoreID,
        TerritoryID,
        AccountNumber,
        ModifiedDate
    from {{ ref('customers_snapshot') }}
    where AddressID is not null
),

-- Get current valid addresses from the already crerated  adress tabel in the silver layer
addresses as (
    select
        AddressID,
        AddressLine,
        City,
        StateProvinceID,
        PostalCode,
         ModifiedDate
    from {{ ref('address_snapshot') }}
    where AddressID is not null
),

-- Join customers with addresses and create sort of primary key
dim_customers as (
    select
    row_number() over (order by c.CustomerID) as customer_sk,
        c.CustomerID,
        c.AddressID,
        c.StoreID,
        c.TerritoryID,
        c.AccountNumber,
        c.ModifiedDate as CustomerModifiedDate,
        a.AddressLine,
        a.City,
        a.StateProvinceID,
        a.PostalCode,
        a.ModifiedDate as AddressModifiedDate
    from customers c
    left join addresses a
        on c.AddressID = a.AddressID
)


select *
from dim_customers
{% if is_incremental() %}
 AND CustomerModifiedDate> (select max(CustomerModifiedDate) from {{this}})

 {% endif %}
 ```
```
with salesorderdetail_snapshot as (
    select
        SalesOrderID,
        SalesOrderDetailID,
        CarrierTrackingNumber,
        OrderQuantity,
        ProductID,
        SpecialOfferID,
        UnitPrice,
        UnitPriceDiscount,
        TotalPrice
    from {{ ref("salesorderdetail_snapshot") }}
    where dbt_valid_to is null
),
lsalesorderheader_snapshot as (
    select
        SalesOrderID,
        RevisionNumber,
        OrderDate,
        DueDate,
        ShipDate,
        Status,
        OnlineOrderFlag,
        SalesOrderNumber,
        PurchaseOrderNumber,
        AccountNumber,
        CustomerID,
        SalesPersonID,
        TerritoryID,
        BillToAddressID,
        ShipToAddressID,
        ShipMethodID,
        CreditCardID,
        CreditCardApprovalCode,
        CurrencyRateID,
        SubTotal,
        TaxAmt,
        Freight,
        TotalDue,
        ModifiedDate as HeaderModifiedDate,
        row_number() over (partition by SalesOrderID order by SalesOrderID) as row_num
    from {{ ref('salesorderheader_snapshot') }}
    where dbt_valid_to is null
)

select
    h.SalesOrderID,
    h.OrderDate,
    h.DueDate,
    h.ShipDate,
    h.Status,
    h.CustomerID,
    h.SalesPersonID,
    h.TerritoryID,
    h.TotalDue,
    d.SalesOrderDetailID,
    d.ProductID,
   
from salesorderdetail_snapshot d
join lsalesorderheader_snapshot h
    on d.SalesOrderID = h.SalesOrderID
where h.row_num = 1
```
```


with product_snapshot as (
    select
        ProductID,
        ProductName,
        ProductNumber,
        MakeFlag,
        FinishedGoodsFlag,
        Color,
        SafetyStockLevel,
        ReorderPoint,
        StandardCost,
        ListPrice,
        ProductSize,
        SizeUnitMeasureCode,
        WeightUnitMeasureCode,
        Weight,
        DaysToManufacture,
        ProductLine,
         ProductClass,
         ProductStyle,
        ProductSubcategoryID,
        ProductModelID,
        SellStartDate,
        SellEndDate,
       
        ModifiedDate
    from {{ ref("product_snapshot") }}
    where dbt_valid_to is null
),

-- Product model snapshot (only current valid records)
product_model_snapshot as (
    select
        ProductModelID,
       
       
        ModifiedDate as ModelModifiedDate
    from {{ ref("productmodel_snapshot") }}
    where dbt_valid_to is null
),

-- Join product + product model
transformed as (
    select
        row_number() over (order by p.ProductID) as Product_SK,  -- Surrogate Key
        p.ProductID,
        p.ProductName,
        p.ProductNumber,
        p.MakeFlag,
        p.FinishedGoodsFlag,
        p.Color,
        p.SafetyStockLevel,
        p.ReorderPoint,
        p.StandardCost,
        p.ListPrice,
        p.ProductSize,
        p.SizeUnitMeasureCode,
        p.WeightUnitMeasureCode,
        p.Weight,
        p.DaysToManufacture,
        p.ProductLine,
          ProductClass,
        ProductStyle,
        p.ProductSubcategoryID,
        p.ProductModelID,
      
        pm.ModelModifiedDate,
        p.SellStartDate,
        p.SellEndDate,
       
        p.ModifiedDate as ProductModifiedDate
    from product_snapshot p
    left join product_model_snapshot pm
        on p.ProductModelID = pm.ProductModelID
)

select *
from transformed
```
```
with salesorderdetail_snapshot as (
    select
        SalesOrderID,
        SalesOrderDetailID,
        ProductID, -- You're missing this join key!
        CarrierTrackingNumber,
        OrderQuantity,
        SpecialOfferID,
        UnitPrice,
        UnitPriceDiscount,
        TotalPrice
    from {{ ref("salesorderdetail_snapshot") }}
    where dbt_valid_to is null
),

product_snapshot as (
    select
        ProductID,
        ProductName,
        ProductNumber,
        MakeFlag,
        FinishedGoodsFlag,
        Color,
        SafetyStockLevel,
        ReorderPoint,
        StandardCost,
        ListPrice,
        ProductSize,
        SizeUnitMeasureCode,
        WeightUnitMeasureCode,
        Weight,
        DaysToManufacture,
        ProductLine,
        ProductClass,
        ProductStyle,
        ProductSubcategoryID,
        ProductModelID,
        SellStartDate,
        SellEndDate,
        ModifiedDate as ProductModifiedDate
    from {{ ref('product_snapshot') }}  
    where dbt_valid_to is null
),

salesorderheader_snapshot as (
    select
        SalesOrderID,
        RevisionNumber,
        OrderDate,
        DueDate,
        ShipDate,
        Status,
        OnlineOrderFlag,
        SalesOrderNumber,
        PurchaseOrderNumber,
        AccountNumber,
        CustomerID,
        SalesPersonID,
        TerritoryID,
        BillToAddressID,
        ShipToAddressID,
        ShipMethodID,
        CreditCardID,
        CreditCardApprovalCode,
        CurrencyRateID,
        SubTotal,
        TaxAmt,
        Freight,
        TotalDue,
        ModifiedDate as HeaderModifiedDate,
        row_number() over (partition by SalesOrderID order by SalesOrderID) as row_num
    from {{ ref('salesorderheader_snapshot') }}
    where dbt_valid_to is null
),

transformed as (
    select
        -- Sales Order Detail fields
        sod.SalesOrderID,
        sod.SalesOrderDetailID,
        
        sod.OrderQuantity,
        sod.SpecialOfferID,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        sod.TotalPrice,

        -- Product fields
        p.ProductID,
       
        p.StandardCost,
        p.ListPrice,

        -- Sales Order Header fields
        
        
        soh.CustomerID,
        soh.SalesPersonID,
        soh.TerritoryID,
        soh.BillToAddressID,
        soh.ShipToAddressID,
        soh.ShipMethodID,
        soh.CreditCardID,
        
        soh.CurrencyRateID,
        soh.SubTotal,
        soh.TaxAmt,
        soh.Freight,
        soh.TotalDue
       
    from salesorderdetail_snapshot sod
    left join product_snapshot p 
        on sod.ProductID = p.ProductID
    left join salesorderheader_snapshot soh 
        on sod.SalesOrderID = soh.SalesOrderID
    where soh.row_num = 1
)

select * from transformed
```











