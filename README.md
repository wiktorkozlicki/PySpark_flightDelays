# PySpark_flightDelays
Basic queries in PySpark, using DataFrame created from CSV file.

The CSV file contains info about flight delays in USA airports. The columns are:
- **date** of flight, that can be mapped to date format
 - **delay** of departue expressed in minutes
 - **distance** between airports
 - **origin** airport, using IATA code
 - **destination** airport, using IATA code

The code with comments is in the flightDelays.py file.

File results.txt contains results returned in the terminal by the program submitted to Spark using command:

    spark-submit flightDelays.py

Source of the file is _Airline On-Time Performance and Causes of Flight Delays_ catalog.
https://catalog.data.gov/dataset/airline-on-time-performance-and-causes-of-flight-delays-on-time-data
