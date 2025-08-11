# Vehicle Sales Profit Analysis  
**Big Data Applications – Project (Sem III)**  
*Arnaav Anand*  
*12/06/2024*  

---

## 1. Introduction  

### 1.1 Project Goal  
This project builds a data processing pipeline using AWS infrastructure to manage and analyze the `car_prices` dataset. The pipeline processes data, extracts insights, and sends notifications about key trends. It integrates with AWS QuickSight for dynamic dashboards and leverages SageMaker Studio to train models predicting profit based on vehicle features. Automation ensures seamless updates while optimizing resources and costs.

### 1.2 Dataset  
The `car_prices` dataset contains 558,838 rows and 16 columns detailing historical car sales. Each row represents a sale with attributes such as year, make, model, trim, body type, transmission, VIN, state of sale, condition, odometer reading, colors, seller info, Manheim Market Report (MMR) value, selling price, and timestamps. This comprehensive data supports analysis of vehicle valuation, market trends, and buyer preferences.

---

## 2. Methodology  

### 2.1 AWS Setup  
An EC2 instance running Amazon Linux 2023 was provisioned with appropriate security groups and IAM roles to access AWS services securely. Billing alerts and IAM policies were configured to monitor usage and permissions. The EC2 instance was linked to AWS Management Console for resource management.

### 2.2 S3 Bucket Creation  
An S3 bucket in the us-east-2 region was created as central storage for the dataset and processed outputs. Proper permissions restricted access to authorized users and the EC2 instance. The raw CSV (`car_prices.csv`) was uploaded to this bucket.

### 2.3 CLI Configurations & Installations  
AWS CLI was installed and configured on the EC2 instance with IAM role authentication. Additional tools installed include Java, PySpark, Hadoop, and Jupyter Notebook, configured in a Python virtual environment for data processing and visualization.

### 2.4 Data Ingestion  
A PySpark session was initiated to load the dataset from S3 into the processing environment. Verification confirmed successful ingestion, making the data available for analysis.

### 2.5 Data Processing  
Spark was used to clean and transform data by calculating vehicle age and profit margin. Sale dates were parsed to extract the sale year, and vehicle age was computed by comparing sale year with the vehicle's manufacturing year. Negative ages were corrected, and null values were removed. Profit margin was calculated as selling price minus MMR value. The final dataset was cleaned of any remaining nulls.

### 2.6 Data Aggregation  
Complex aggregation queries were executed using Spark submit scripts outside Jupyter Notebook to handle computational load effectively.

### 2.7 Storing Processed Data to S3  
Processed data was written back to S3 using Spark's write function. The output files were managed manually to fit the storage structure.

### 2.8 Data Analysis Using SQL  
Spark SQL queries provided insights into data distributions and key metrics for further analysis.

### 2.9 Machine Learning with AWS SageMaker  
Processed data was split into training and test sets and uploaded to SageMaker. A regression model was trained to predict profit, with model training lasting approximately four hours.

### 2.10 Visualization  
Amazon QuickSight connected to the processed data in S3 to create an interactive dashboard showing trends like average selling price, profit over time, vehicle condition, and age across makes. The dashboard was configured for periodic data refreshes to display up-to-date insights.

### 2.11 Automation of the Pipeline  
The entire pipeline was automated using a daily cron job on the EC2 instance to fetch, process, and store data. AWS SNS was integrated for notifications about pipeline progress and errors, reducing manual intervention.

---

## 3. Results  

### 3.1 SageMaker Autopilot  
The best-performing model showed a Root Mean Square Error (RMSE) of 62.27 and an R² value of 99.87%, indicating highly accurate profit predictions. Features such as MMR and selling price were most influential.

### 3.2 Visualization  
The QuickSight dashboard offered key insights:  
- Luxury brands had higher average selling prices.  
- Profitability declined over recent years.  
- Older vehicles tended to yield lower profits.  
- Vehicle condition varied notably across makes.

---

## 4. Conclusion  

The analysis revealed how vehicle attributes like make, age, and condition influence profitability in car sales. Luxury brands commanded higher prices, and a downward trend in profit was observed, indicating shifting market dynamics. Using AWS tools for data processing, visualization, and machine learning enabled efficient handling of large datasets and production of accurate predictive models. Pipeline automation ensured consistent, timely updates and reduced manual effort. This project demonstrates the power of combining big data and cloud services for actionable business insights.

---
