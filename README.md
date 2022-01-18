# load_datawarehouse
 Load data into common cloud warehousing SaaSs.

## Premise of this module
 The industry's move from self hosted SQL to Data Warehouses on the Cloud marked a divergence of standards. While SQL distributions have their unique characteristics and data types etc, they are in broad terms interoperable. This is different from a contemporary migration among BigQuery, Redshift and Snowflake, each of which has independently developed API and data structures. As these service providers aim to provide unique features to entice existing on-prem database users, these unique traits will further reduce interoperability in the long run, not unlike the browser wars of early internet during which the supposed standard of HTML/JS/CSS diverge into a chaotic scene of unpredictable behaviours.

 Combined with the fact that these are Cloud services subject to outages, planned obsolescence and non-fixed pricing structures, it is evident that apps built solely upon one single SaaS implementation will be significantly disadvantaged when that SaaS no longer provide favourable terms.

 This module sets out to address this by providing a transparent layer between the SaaS bespoke APIs and the actual application, allowing developers to switch from one platform to another by changing the subclass of a `Warehouse` instance.
 
 The difficulty is that out of the 3, only GCP provides a free tier for permanent use of BigQuery, while AWS and Snowflake both offers trial periods only. With this being a solo hobbyist project, completing the subclasses for all 3 platforms will be very time consuming; so any help would be appreciated.



## Modules
### Cross Platform
- load_datawarehouse.api
- load_datawarehouse.config
- load_datawarehouse.data
- load_datawarehouse.schema

### Platform Specific
- load_datawarehouse.bigquery
- load_datawarehouse.redshift
- load_datawarehouse.snowflake
