# Databricks notebook source

# COMMAND ----------

import requests
import json
  
def get_iics_subtasks(run_id,username="",password=""):
  
  url = "https://usw5.dm-us.informaticacloud.com/active-bpel/services/tf/status/{}".format(run_id)
  
  if len(username) == 0 or len(password) == 0 :
      username = "user@mercy.dev"
      password = "******"
  
  response = requests.get(url, auth=(username, password))
  
  if response.status_code == 200:
    payload = response.json()
    if int(payload["subtasks"]) > 0 :
      subtasks_meta = payload["subtaskDetails"]["details"]["tasks"]
      subtasks_list = [subtask['assetName'] for subtask in subtasks_meta]
      return subtasks_list
    else:
      return []
  else:
    raise Exception("Error : Making GET Request to IICS Task Status Endpoint with RunId : {} Failed..".format(run_id))
       

# COMMAND ----------

run_ids = ["713658850251825152","713665145700806656","713665506922651648","713658843532550144"]
light_weight_tables = []

for run_id in run_ids:
  light_weight_tables.extend(get_iics_subtasks(run_id))
  
print(light_weight_tables)



