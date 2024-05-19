# Phonepe-Pulse-Data-Visualization
# Phonepe Pulse Data Visualization and Exploration: A User-Friendly Tool Using Streamlit and Plotly

## What is PhonePe Pulse?
  The [PhonePe Pulse website](https://www.phonepe.com/pulse/explore/transaction/2022/4/) showcases more than 2000+ Crore transactions by consumers on an interactive map of India. With over 45% market share, PhonePe's data is representative of the country's digital payment habits.The insights on the website and in the report have been drawn from two key sources - the entirety of PhonePe's transaction data combined with merchant and customer interviews. The report is available as a free download on the [PhonePe Pulse website](https://www.phonepe.com/pulse/explore/transaction/2022/4/) and [GitHub](https://github.com/PhonePe/pulse).

## Overview
This project is to develop a solution that extracts, transforms, and visualizes data from the Phonepe Pulse GitHub repository. 

## Tools Installed  
PyCharm Community Edition 2023.3.3 ,
PgAdmin 4(PostgreSQL) 

## Required Libraries:

 1. [Plotly](https://plotly.com/python/) - (To plot and visualize the data)
 2. [Pandas](https://pandas.pydata.org/docs/) - (To Create a DataFrame with the scraped data)
 3. psycopg2 - (To store and retrieve the data)
 4. [Streamlit](https://docs.streamlit.io/library/api-reference) - (To Create Graphical user Interface)
 5. json - (To load the json files)

## ETL and EDA Process 

**Data Extraction:** Scripting to clone the repository and collect data.

**Data Transformation:** Using Python and Pandas to clean and structure the data.

**Database Insertion:** Storing transformed data in a POSTGRESQL database.

**Dashboard Creation:** Using Streamlit and Plotly to build an interactive dashboard.

**Data Retrieval:** Fetching data from the database to dynamically update the dashboard.

## User Guide 

**Step 1** - Home Tab provides a brief overview of the project.
**Step 2** - Data Exploration Tab contains three section "Aggregated Analysis","Map Analysis","Top Analysis" under which you can filter on the methods "Aggregated Insurance Analysis","Aggregated Transaction Analysis","Aggregated User Analysis", year "2020-2023" and the quarter "1-4" to get your visualization.
**Step 3** - Top charts Tab has a dropdown consist of 12 following question to get better insights on our dataset.

                             "1. Transaction Amount & Count for Aggregated Insurance",
                             "2. Transaction Amount & Count for Map Insurance",
                             "3. Transaction Amount & Count for Top Insurance",
                             "4. Transaction Amount & Count for Aggregated Transaction",
                             "5. Transaction Amount & Count for Map Transaction",
                             "6. Transaction Amount & Count for Top Transaction",
                             "7. Transaction Count for Aggregated User",
                             "8. Registered Users of Map User",
                             "9. App Opens for Map User",
                             "10. Registered Users of Top User",
                             "11. Brands & Transaction Count of Aggregated User",
                             "12. Transaction count per Transaction type for Aggregated Transaction"
