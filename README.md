# 📊 SQL ETL & Database Backup Project

This project demonstrates two core database management systems built using SQL Server:

* Data Cleaning & ETL Process
* Database Backup & Automation System

---

## 🔹 1. ETL (Data Cleaning) Project

This module focuses on transforming raw customer data into a clean and usable dataset.

### Key Features:

* Layered architecture (Raw → Staging → Clean → Reject)
* Data validation (email, phone, null checks)
* Data standardization and transformation
* Duplicate handling
* Logging system (ETL_Log table)
* Stored procedure-based ETL workflow

### Result:

* Clean data ready for analysis
* Invalid data separated with error reasons

---

## 🔹 2. Database Backup & Automation Project

This module automates SQL Server backup processes and tracks system behavior.

### Key Features:

* Full backup and differential backup procedures
* Logging system (Backup_Log)
* Alert system for failed operations (Backup_Alert)
* Error handling using TRY...CATCH
* Reporting stored procedures
* Backup validation using system tables (msdb.dbo.backupset)

### Scenarios Implemented:

* Successful backup
* Differential backup
* Simulated failure scenario

---

## ⚙️ Technologies Used

* Microsoft SQL Server
* T-SQL
* Stored Procedures

---

## 📁 Project Structure

* `/etl` → ETL scripts
* `/backup` → Backup scripts

---

## 🚀 How to Run

1. Run ETL script
2. Execute backup procedures
3. Review logs and reports

---

## 📌 Purpose

This project was developed to demonstrate real-world database management concepts such as:

* Data quality improvement
* Process automation
* Monitoring and logging systems
