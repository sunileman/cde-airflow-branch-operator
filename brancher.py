import requests

HOST = "https://xxxxxx.se-sandb.a465-9q4k.cloudera.site"
USERNAME = "sunilemanjee"
API_KEY = "xxxxxxxx"
PROJECT_NAME = "workflow-orchestration"
jdict = "resources/dic.json"

url = "/".join([HOST, "api/v1/projects", USERNAME, PROJECT_NAME, "files", jdict])


def deterministic():
    res = requests.get(url, headers={"Content-Type": "application/json"}, auth=(API_KEY, ""))
    print(res.text)
    if res.json()['sale-amount'] > 5:
        branch_name = 'bigSale'
    else:
        branch_name = 'smallSale'
    return branch_name


print(deterministic())
