
## IceCreamIQ Project

### Overview

Welcome to the IceCreamIQ project repository! This project aims to revolutionize how Soca Ice Cream leverages its data for strategic decision-making. By implementing a robust data warehouse and analytics stack, Soca Ice Cream will gain valuable insights into its sales performance and customer behavior, ultimately driving business growth and enhancing customer satisfaction.

### Project Goals

The primary goals of the IceCreamIQ project are:

- **Data Warehouse Creation**: Design and implement a scalable data warehouse using SQL Server 2019 as the primary RDBMS.
- **ETL Automation**: Utilize Apache Airflow for orchestrating and automating ETL processes, ensuring timely and accurate data processing.
- **Cloud Data Warehousing**: Leverage Snowflake as the cloud data warehouse for storing and querying large volumes of sales data.
- **Data Modeling with dbt**: Implement star schema data models using dbt to facilitate efficient analytics and reporting.
- **Data Visualization with Tableau**: Utilize Tableau for creating interactive dashboards and reports to visualize sales performance and trends.

#### Technologies Used

1. **SQL Server 2019**: Primary RDBMS for storing and managing sales data.
2. **Apache Airflow**: Orchestration tool for automating ETL processes.
3. **Snowflake**: Cloud data warehouse for storing and querying sales data.
4. **dbt (Data Build Tool)**: Data modeling tool for creating star schema data models.
5. **Tableau**: Data visualization tool for creating interactive dashboards and reports.

### Dimensional Modeling

- **Two important requirements**: Key word `simplicity`
	- Deliver data in an understanable format
	- Deliver fast query performance
	
- **Elements of Dimensional Model**
	- Facts: The measurements/metrics or fact from your business process
	- Dimensions: For providing the context of a business process event
	- Attributes: Various characteristics of a dimension
	- Star Schema: Sales

### Dimension

- **Characteristics of Dimension tables**
	- No limit for the number of attributes in a dimension table
		- Common to have tables with 50 to 100 attributes
		- some dimension tables have only a handful of attributes
	- Have fewer rows than fact tables
		- but can be much wider
	- Defined by a single primary key
		- Basis for the referential integrity with the fact table
	- Denormalized
		- Flattened many-to-one relationship withi a single dimension table
#### Dimensional Design
*Focus on understanding the deliverablles of the project: `Consist of four steps`*

- **Step 1: Select the business Process**
	- Low-level activites performed by an organization are identified by listining carefully to business users
		- Chaaracteristics:
			- Expressed as verbs
			- Are supported by an operational system
			- Generate KPIs
- **Step 2: Identify the Grain** 
	- Specify the detail level of a business process we want to measure: How do you describe a single row in a fact table
		- For instance:Grain declarations are expressed in business terms
			- Example:Sales/day/order
			- Grain: One row for every finished order
			- Questions to ask: What was the most sold product? 
- **Step 3: Identify the diensions**
	- How do business users describe the data resulting from the process
	- who, what, where, when, why, how
		- Examples:
			- Date
			- Product
			- Customer
			- Employee
- **Step 4: Identify the facts**
	- What is the process measuing?
	- All candidate facts must be ture to the grain for that fact
	- Fact with different grains are split in separate tabels
		- Sales price
		- Sales quantity (or Units sold)
#### Who, What, Where, When, How, Why

- **Who**: Customers from over 100 countries and 200 cities are buying Soca Ice Cream products.
- **What**: Popular products include Bacon ice cream, Birthday Cake, Black Raspberry, and many more listed in the Products table.
- **Where**: Products are sold in various locations globally, as indicated by the presence of over 200 cities and 100 countries in the customer base.
- **When**: Sales patterns can be analyzed over time to determine peak sales periods.
- **How**: Customers purchase products by going to retail stores, selecting products, checking out with cashiers, confirming payments, and then leaving with the product.
- **Why**: The success of products could be attributed to factors such as taste, marketing, seasonal demand, and customer preferences.

By leveraging these technologies and best practices, Soca Ice Cream aims to build a modern and efficient data infrastructure that empowers its team to make informed decisions and drive business growth.
##### Getting Started: Make sure you have the following
- **Step 1: Local Develoment Stack**
	- Python: 
	- Code Editor: https://code.visualstudio.com/

- **Step 2 Hosted paltforms:**
	- Airlow: https://airflow.apache.org/docs/
	- Github: https://github.com/
	- DBT: https://www.getdbt.com/
	- Snowflake: https://www.snowflake.com/


- **To get started with dbt please click the link** https://github.com/Jayboy628/dbt_claims_report/tree/main


### Data Sources

- **POS**: Internal company database Sql Server 2019.
	- **Database**: SocaIceCream.
		- **Tables**:
			- Addresses
				- Cities
				- Countries
				- Customers
				- Employees
				- Ingredients
				- InventoryItems
				- InventoryTransactions
				- OrderLines
				- Orders
				- PackageTypes
				- PaymentTypes
				- ProductCategories
				- ProductDepartments
				- Products
				- ProductSubcategories
				- Promotions
				- Provinces
				- Recipes
				- Stores
				- UnitsOfMeasure

### Target Environments

- **Development**: Schema: `DBT_SBROWN` (One per developer)

- **Production**: Schema:
 `STAGING` (1:1 with each source-system table) 
 
 `MARTS` (Fully transformed and joined models)

### Visualization Environments

- **Data Visualization with Tableau**:
	- **To get started with Tableau please click the link: https://github.com/Jayboy628/tableau_soca_icecream

### Contributors

- Shaunjay Brown ( BI Developer)
- Shaunjay (Data Engineer Developer)