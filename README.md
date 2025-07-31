# Google Sheet Categorizer via API

This program connects to a Google Sheet using the Google Sheets API created via Google Cloud Console. Its main purpose is to analyze and categorize traffic sources.

## 🔧 Features

- Connects to Google Sheets via the official API
- Reads data row by row
- Automatically categorizes orders based on source:
  - **Google Ads**
  - **Google Organic**
  - **Meta Ads**
  - **Meta Organic**
  - **Others**
- Supports batch processing to stay within API limits

## ⚙️ API Limitations

The Google Sheets API allows a maximum of 1000 requests per 60 seconds. To handle this, a batching mechanism with a timer is implemented to ensure smooth operation without exceeding quotas.

## 🚀 How to Use

1. Create a project in the [Google Cloud Console](https://console.cloud.google.com/).
2. Enable **Google Sheets API** and **Google Drive API**.
3. Create a service account and download the `credentials.json` file.
4. Share access to your Google Sheet with the service account email.
5. Run the script to process the data and generate categorized stats.

## 📁 Example

```python
# Pseudocode
for row in sheet:
    if source == 'google' and medium == 'cpc':
        category = 'Google Ads'
    elif source == 'google':
        category = 'Google Organic'
    elif source in ['facebook', 'instagram'] and medium == 'paid':
        category = 'Meta Ads'
    elif source in ['facebook', 'instagram']:
        category = 'Meta Organic'
    else:
        category = 'Others'
