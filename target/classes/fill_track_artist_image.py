"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import date,datetime, timedelta
import os
import getpass

from airflow.operators import WaitGCSOperator


default_args = {
    'owner': 'alexey.rudenko2002@umusic.com',
    'depends_on_past': False,
    #'start_date': datetime(2017, 9, 26),
    #'start_date': datetime.now(),
    #'email': ['airflow@airflow.com'],
    'email': ['arudenko2002@yahoo.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    #'schedule_interval': '30,*,*,*,*',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

project = '{{var.value.project}}'
runner = '{{var.value.runner}}'
from_mongodb_users = '{{var.value.from_mongodb_users}}'
artist_track_images = '{{var.value.artist_track_images}}'
playlist_geography = '{{var.value.playlist_geography}}'
playlist_track_action = '{{var.value.playlist_track_action}}'
product = '{{var.value.product}}'
playlist_track_history = '{{var.value.playlist_track_history}}'
playlist_history = '{{var.value.playlist_history}}'
streams = '{{var.value.streams}}'
canopus_resource = '{{var.value.canopus_resource}}'
canopus_name = '{{var.value.canopus_name}}'
mail_alerts_output = '{{var.value.mail_alerts_output}}'
temp_directory = '{{var.value.temp_directory}}'
whom = '{{var.value.whom}}'
gmail = '{{var.value.gmail}}'
mongoDB = '{{var.value.mongodb}}'
outputfile = '{{var.value.outputfile}}'
inputfile = '{{var.value.inputfile}}'
destination_table = '{{var.value.destination_table}}'
environment='{{var.value.environment}}'
#executionDate='{{var.value.executionDate}}'
schedule = '0 15 * * *'
sleep_seconds = '{{var.value.sleep}}'
alsome = '{{var.value.alsome}}'
mongodb = '{{var.value.step_mongodb}}'
enrichment = '{{var.value.step_enrichment}}'
major_sql = '{{var.value.step_major_sql}}'

#schedule = '{{var.value.schedule_interval}}'
#if executionDate is "today":
executionDate=str(datetime.now())[:10]

fillTrackArtistImage = 'java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.FillArtistTrackImage' \
                       + ' --executionDate '+executionDate +"" \
                       + ' --project ' + project + ' --runner ' + runner + ' --from_mongodb_users ' + from_mongodb_users + ' --artist_track_images ' + artist_track_images +  '' \
                       + ' --playlist_geography ' + playlist_geography + ' --playlist_track_action ' + playlist_track_action +  '' \
                       + ' --product '+product+' --playlist_track_history '+playlist_track_history+' --playlist_history '+playlist_history + ' --streams ' + streams + '' \
                       + ' --canopus_resource '+canopus_resource+' --canopus_name '+canopus_name+'' \
                       + ' --outputfile gs://umg-dev/swift_alerts --temp_directory '+temp_directory
print fillTrackArtistImage

sleep_pause = 'sleep 600'
print sleep_pause

actualDate = str(datetime.now()+timedelta(days=-1))[:10]
loadTrackArtistImage = "bq load -F '{' umg-dev:swift_alerts.artist_track_images " \
                       "gs://umg-dev/swift_alerts/imagesAPI_"+actualDate+"/images_"+actualDate+".csv* " \
                        "canopus_id:int64,artist_name:string,isrc:string,track_uri:string,artist_uri:string,track_image:string,artist_image:string,last_update:timestamp"
print loadTrackArtistImage

bucket="umg-dev"
prefix = "swift_alerts/imagesAPI_"+actualDate+"/images_"

dagTask = DAG(
    'artist_track_images', default_args=default_args,
    #schedule_interval='30 * * * *',
    start_date=datetime(2018, 02, 06, 0, 0, 0),
    schedule_interval='0 10 * * *')

# t10
t0 = BashOperator(
    task_id='api_image_table',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.FillArtistTrackImage --executionDate 2017-11-05 --project umg-dev --runner DataflowRunner --temp_directory gs://umg-dev/temp --outputfile gs://umg-dev/swift_alerts',
    bash_command=fillTrackArtistImage,
    dag=dagTask)

GCS_Files = WaitGCSOperator(
    task_id='GCS_Files',
    bucket=bucket,
    prefix=prefix,
    number="2",
    google_cloud_storage_conn_id="google_cloud_default",
    dag=dagTask
)

t2 = BashOperator(
    task_id='load_image_table',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.FillArtistTrackImage --executionDate 2017-11-05 --project umg-dev --runner DataflowRunner --temp_directory gs://umg-dev/temp --outputfile gs://umg-dev/swift_alerts',
    bash_command=loadTrackArtistImage,
    dag=dagTask)

t0>>GCS_Files>>t2


