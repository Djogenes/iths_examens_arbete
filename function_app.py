from azure.storage.blob import BlobServiceClient
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from openai import OpenAI
import azure.functions as func
import datetime
import logging
import requests
import markdown
import smtplib
import json
import os
import fireducks.pandas as pd
import openmeteo_requests
import requests_cache
import requests
from retry_requests import retry



connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "dailyreport"
blob_name = "daily_report.json"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)


def api_caller() -> dict:
    """
    Fetches data from an API using credentials and parameters from environment variables.
    
    Returns:
        json: A list of records retrieved from the API, or an empty list if an error occurs.
    
    Logs:
        - Error if API key is missing.
        - Number of records retrieved if successful.
        - Error details if the API call fails.
    """

    url = os.getenv("DATA_URL")
    api_key = os.getenv("DATA_KEY")

    if not api_key:
        logging.error("API_KEY is missing in the environment variables")
        return {}
        
    headers ={
        "ApiKey": api_key,
        "Content-Type": "application/json"
    }

    start_date = str(datetime.date.today() + datetime.timedelta(days=-1))
    end_date = str(datetime.date.today())
    params = {
        "startDate": f"{start_date} 00:00",
        "endDate": f"{end_date} 00:00"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Retrieved {len(data)} records from API")
            return data
        
        else:
            logging.error(f"API call error: {response.status_code}, {response.text}")
            return {}

    except Exception as e:
        logging.error(f"API call error: {str(e)}")
        return {}

def scriba(data) -> str:
    """
    Generates a daily report in Markdown format based on the provided data.

    This function uses the OpenAI API to analyze and compile a report 
    according to a predefined structure. If the API key is missing or an error occurs, 
    it logs the issue and returns an error message.

    Args:
        data (dict): JSON structure containing events to be included in the report.

    Returns:
        str: A generated report in Markdown format or an error message if an issue occurs.
    """

    try:
        api_key = os.getenv("AI_KEY")
        if not api_key:
            logging.error("API-key missing!")
            raise ValueError("API-key missing!")

        client = OpenAI(api_key=api_key)

        
        system_prompt = """
            Du är en noggrann och effektiv sekreterare. Din uppgift är att sammanställa en daglig rapport 
            baserad på händelser som användaren tillhandahåller.

            ### **Rapportstruktur (Markdown-format)**  

            För varje **Site** i JSON-filen ska en separat rapport skapas i exakt **Markdown-format** enligt följande:

            ```markdown
            # Daglig rapport för [Site] - [Datum]

            ## 🟢 Viktigaste händelser:
            ### 🔹 Ronderingar:
            - **[Tid]** – [Var - Händelsebeskrivning]

            ### 🏪 Butiksbesök:
            - **[Tid]** – [Butik - Anledning]

            ### 🚷 Bortvisning:
            - **[Tid]** – [Var - Beskrivning]

            ### ⚠️ Hänvisningar:
            - **[Tid]** – [Var - Beskrivning]

            ### ⏳ Öppettider kontroll:
            - **[Tid]** – [Butik - Händelse]

            ## 🟡 Övriga händelser:
            - **[Tid]** – [Var - Händelsebeskrivning]

            ## 🔵 Sammanfattning:
            [Kort reflektion över dagens händelser och rekommendationer.]

            """
        data_string = json.dumps(data, ensure_ascii=False, indent=2)
        user_prompt = f"Analysera och skapa rapport på följande data: {data_string}"

        logging.info("Sending the request to OpenAI API...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=3000,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )

        report = response.choices[0].message.content
        logging.info("Report created successfully.")
        return report
    
    except Exception as e:
        logging.error(f"Error while creating report: {e}")
        return f"There was an error: {e}"

def download_blob() -> list:
    """
    Downloads a blob from Azure Blob Storage, reads its content, and decodes it as JSON.

    Returns:
        list or dict: Parsed JSON data from the blob, or an empty list in case of an error.

    Logs:
        - Attempt to download the blob.
        - Successful retrieval and decoding of blob data.
        - Successful JSON parsing.
        - Any JSON decoding errors or unexpected exceptions.
    """

    # Connects to Azure Blob Storage and retrieves a specific container 


    try:
        logging.info(f"Attempting to download blob: {blob_name}")

        blob_client = container_client.get_blob_client(blob_name)
        downloaded_blob = blob_client.download_blob()
        logging.info("Downloaded blob successfully")

        text_data = downloaded_blob.readall().decode('utf-8')
        logging.info("Downloaded and decoded blob data successfully")

        report_data = json.loads(text_data)
        logging.info("Parsed JSON successfully")
       
        return report_data   
    
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        return []
    
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        logging.error(f"Error type: {type(e)}")
        return []


def send_email(content) -> None:
    """
    Sends an email with the given content as an HTML-formatted message.

    The function converts the content from Markdown to HTML, constructs an 
    email using the sender's credentials, and sends it via mail's SMTP server.

    Args:
        content (str): The email content in Markdown format.

    Returns:
        None if email is successfully sent, or logs an error if sending fails.
    """

    if not content:
        logging.error("No content to send.")
        return {}

    USER_EMAIL = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    if not USER_EMAIL or not EMAIL_PASSWORD:
        logging.error("Email credentials missing")
        return
    
    html_content = markdown.markdown(content)

    msg= MIMEMultipart()
    msg["From"] = USER_EMAIL
    msg["To"] = USER_EMAIL
    msg["Subject"] = "Daily Report"    

    msg.attach(MIMEText(html_content, "html"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(USER_EMAIL, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        logging.info("Email sent!")
    except Exception as e:
        logging.error(f"Error sending email: {e}")


app = func.FunctionApp()

@app.timer_trigger(
    schedule="0 1 0 * * *",
    arg_name="myTimer", 
    run_on_startup=True,
    use_monitor=True)
def fetch_digester(myTimer: func.TimerRequest) -> None:
    """
    Fetches and processes daily report data triggered by a timer event.

    This function retrieves data from an API, generates a report, and appends it 
    to an existing report stored in Azure Blob Storage. If no previous report 
    exists, it creates a new one. The updated report is then uploaded back to 
    the blob storage and sent via email.

    Args:
        myTimer (func.TimerRequest): Timer trigger object to schedule execution.

    Logs:
        - Notifies if the timer is past due.
        - Logs successful blob upload or errors if encountered.
    """
    
    if myTimer.past_due:
        logging.info("The timer is past due!")
    logging.info("Python timmer trigger function executed")
    

    data = api_caller()    
    report = scriba(data)

    today = str(datetime.date.today() + datetime.timedelta(days=-1))
    report_json={
        "Timestamp": today,
        "Category": "report",
        "Type": "daily",
        "Content": report 
    }

    report_data = download_blob()
    if report_data:
        report_data.append(report_json)
    else:
        report_data = [report_json]
    
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json.dumps(report_data, ensure_ascii=False), overwrite=True)
        logging.info(f"Successfully uploaded updated report to blob {blob_name}")

        send_email(report_json["Content"])

    except Exception as e:
        logging.error(f"Error uploading updated blob: {e}")


def weather_api_caller():
    """
    Calls Open-Meteo API for historical weather data.

    This function retrieves data - observed weather, temperature (degrees C) and rain (mm)- per hour 
    for the last 24 hours and saves it as a structured json file.

    Returns:
        pd.DataFrame: A DataFrame containing the weather data.
    """

    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"

    lat = 59.33
    long = 18.00

    start_date = str(datetime.date.today() + datetime.timedelta(days=-1))
    end_date = str(datetime.date.today())
    start_formatted = start_date.strftime("%Y-%m-%d")
    end_formatted = end_date.strftime("%Y-%m-%d")

    params = {
	"latitude": lat,
	"longitude": long,
	"start_date": start_formatted,
	"end_date": end_formatted,
	"hourly": ["temperature_2m", "rain", "weather_code"]
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
    except requests.exceptions.RequestException as e:
        print(f"Error making SER API request: {e}")
        return None

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_rain = hourly.Variables(1).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(2).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["rain"] = hourly_rain
    hourly_data["weather_code"] = hourly_weather_code

    weather_df = pd.DataFrame(data = hourly_data)

    weather_df['datetime'] = weather_df['date'].dt.strftime('%Y-%m-%d %H:%M')
    weather_df['temp'] = weather_df['temperature_2m'].round(0)

    weather_df = weather_df.drop(columns=['date'])
    weather_df = weather_df.drop(columns=['temperature_2m'])

    weather_df = weather_df[['datetime', 'temp', 'rain', 'weather_code']]

    weather_code_descriptions = {0: 'Klart',
                             1: 'Mestadels klart',
                             2: 'Devlis molnigt',
                             3: 'Molnigt',
                             45: 'Dimmigt',
                             48: 'Dimma och rimfrost',
                             51: 'Lätt duggregn',
                             53:'Duggregn',
                             55: 'Kraftigt duggregn',
                             56: 'Lätt underkylt duggregn',
                             57: 'Underkylt duggregn',
                             61: 'Lätt regn',
                             63: 'Regn',
                             65: 'Kraftigt regn',
                             66: 'Lätt underkylt regn',
                             67: 'Underkylt regn',
                             71: 'Lätt snöfall',
                             73: 'Snöfall',
                             75: 'Kraftigt snöfall',
                             77: 'Snökorn',
                             80: 'Lätta skurar',
                             81: 'Skurar',
                             82: 'Kraftiga skurar',
                             85: 'Lätta snöskurar',
                             86: 'Snöskurar',
                             95: 'Åskväder',
                             96: 'Lätt åskväder med hagel',
                             99: 'Åskväder med hagel'}
    
    final_weather_df = weather_df.copy()
    final_weather_df['weather_description'] = final_weather_df['weather_code'].map(weather_code_descriptions)

    weather_dict = {}
    for _, row in final_weather_df.iterrows():
        weather_dict[row['datetime']] = {
            'temp': row['temp'],
            'rain': row['rain'],
            'weather_code': row['weather_code'],
            'weather_description': row['weather_description']
        }

    # Save to JSON with proper formatting
    with open('weather_data/weather_data.json', 'w', encoding='utf-8') as f:
        json.dump(weather_dict, f, ensure_ascii=False, indent=4)

    return final_weather_df
