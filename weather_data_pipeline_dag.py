from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python_operator import PythonOperator
import json
import pandas as pd
import boto3
from io import StringIO





def kelvin_to_celsius(temp_in_kelvin):
    return temp_in_kelvin - 273.15

def tl_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_data")

    # Extract coordinates
    longitude = data["coord"]["lon"]
    latitude = data["coord"]["lat"]
    
    # Basic info
    city = data["name"]
    country = data["sys"]["country"]
    weather_description = data["weather"][0]['description']
    weather_main = data["weather"][0]['main']
    
    # Temperature data in Celsius
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    feels_like_celsius = kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    
    # Other meteorological data
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    
    # Wind data
    wind_speed = data["wind"]["speed"]
    wind_direction = data["wind"].get("deg")
    wind_gust = data["wind"].get("gust")
    
    # Visibility
    visibility = data["visibility"]
    
    # Clouds
    clouds_percentage = data["clouds"]["all"]
    
    # Time calculations
    local_time = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    local_sunrise = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    local_sunset = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])
    
    
    transformed_data = {
        "city": city,
        "country": country,
        "longitude": longitude,
        "latitude": latitude,
        "weather_description": weather_description,
        "weather_main": weather_main,
        "temperature(celsius)": temp_celsius,
        "feels_like(celsius)": feels_like_celsius,
        "min_temp(celsius)": min_temp_celsius,
        "max_temp(celsius)": max_temp_celsius,
        "pressure(hPa)": pressure,
        "humidity(%)": humidity,
        "wind_speed(meter/sec)": wind_speed,
        "wind_direction(degrees)": wind_direction,
        "wind_gust(meter/sec)": wind_gust,
        "visibility(meter)": visibility,
        "clouds(%)": clouds_percentage,
        "time_of_record": local_time,
        "local_sunrise_time": local_sunrise,
        "local_sunset_time": local_sunset
    }
    
    df_data = pd.DataFrame([transformed_data])

    # Generate S3 file path
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")

    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df_data.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3 = boto3.client('s3')
    s3.put_object(Bucket='weatherdata-store', Key=f"{dt_string}.csv", Body=csv_buffer.getvalue())

    print(f"Uploaded CSV directly to S3 with key: {dt_string}.csv")
    



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 18),
    'email': ['chanidu1214@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('weather_data_pipeline_dag',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

        is_api_ready = HttpSensor(
        task_id='is_api_ready',
        http_conn_id='openweathermap_api',  
        endpoint='data/2.5/weather?q=Colombo,LK&appid=<USE-API-KEY>'  
        )

        extract_data = HttpOperator(
        task_id = 'extract_data',
        http_conn_id = 'openweathermap_api',
        endpoint='data/2.5/weather?q=Colombo,LK&appid=<USE-API-KEY>',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

        transform_load_data = PythonOperator(
        task_id= 'transform_load_data',
        python_callable=tl_data
        )


        is_api_ready >> extract_data >> transform_load_data