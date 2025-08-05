# DriveCam Gas Card Analyzer

This Rails application analyzes **DriveCam telematics data** against **WEX Gas Card transactions** to detect potentially fraudulent fuel purchases. It cross-references transaction details (VIN, driver, time, and merchant address) with vehicle GPS data from the **Lytx API** and generates an Excel report summarizing discrepancies.

---

## Features

- **VIN to Vehicle ID Mapping**: Fetches and caches vehicle IDs from the Lytx API.
- **Transaction Verification**: Compares gas card transactions against vehicle GPS coordinates.
- **Distance Calculation**: Flags purchases made >1000 ft from the vehicle’s location.
- **Partial Report Handling**: Saves analysis state and supports resume after errors/timeouts.
- **Excel Report Export**: Generates `.xlsx` report with categorized sheets:
  - Missing Vehicle IDs
  - Missing Coordinates
  - Flagged Transactions
  - Passed Transactions
- **Progress Tracking**: Real-time progress bar and elapsed time in the UI.
- **Error Handling**: Displays last processed row and allows quick restart.

---

## Prerequisites

- **Ruby** 3.x  
- **Rails** 7.x  
- **PostgreSQL** (or SQLite for local testing)  
- **Lytx API Access** (Bearer token)  
- **Google Maps API Key** (for geocoding merchant addresses)  
- **Node/Yarn** for asset pipeline (Rails 7 default)  

---

## Setup

1. **Clone Repository**
   ```bash
   git clone https://github.com/wyatt-ogle-lpc/drivecam-analyzer.git
   cd drivecam-analyzer
   
Install Dependencies
bundle install

Set Environment Variables
Create .env or use your preferred secrets manager:
GOOGLE_API_KEY=your-google-maps-api-key

Start Server
rails server

Access via http://localhost:3000.

Usage
Obtain a Bearer Token from Lytx API (expires hourly).
Enter token in the UI and upload your Gas Card transaction file (.xlsx).
Configure row limits and offsets as needed.
Start analysis — progress bar updates automatically.
Download generated Excel report upon completion.

File Naming Convention
Reports are named dynamically based on:
Month & Year from uploaded file name (e.g., 07_2025)
Row range analyzed

Example:
07_2025_DriveCam_Gas_Report_2_to_150.xlsx

