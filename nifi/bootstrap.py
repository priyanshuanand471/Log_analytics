import time,requests
BASE='http://nifi:8080/nifi-api'
s=requests.Session()
for i in range(90):
    try:
        r=s.get(BASE+'/flow/about',timeout=5)
        if r.ok:break
    except requests.RequestException:pass
    time.sleep(3)
else:raise RuntimeError('NiFi API not ready')
root=s.get(BASE+'/flow/process-groups/root',timeout=15).json()['processGroupFlow']['id']
processors=s.get(BASE+'/flow/processor-types',timeout=20).json()['processorTypes']
def type_for(suffix):
    matches=[p['type'] for p in processors if p['type'].endswith(suffix)]
    if not matches:raise RuntimeError('Processor type not found: '+suffix)
    return matches[0]
def post(url,obj):
    r=s.post(BASE+url,json=obj,timeout=25);r.raise_for_status();return r.json()
def put_rev(url,obj):
    r=s.put(BASE+url,json=obj,timeout=25);r.raise_for_status();return r.json()
def create(name,ptype,x,y):
    return post('/process-groups/'+root+'/processors',{'revision':{'version':0},'component':{'type':ptype,'name':name,'position':{'x':x,'y':y}}})

def configure(entity,properties,auto):
    pid=entity['id'];rev=entity['revision']
    return put_rev('/processors/'+pid,{'revision':rev,'component':{'id':pid,'config':{
        'properties':properties,'autoTerminatedRelationships':auto}}})

# Avoid duplicate flows if this script is re-run.
existing=s.get(BASE+'/flow/process-groups/'+root,timeout=20).json()['processGroupFlow']['flow']['processors']
if any(p['component']['name']=='LogFileTail' for p in existing):
    print('NiFi flow already present; open UI to inspect and start processors.');raise SystemExit(0)

tail=create('LogFileTail',type_for('.TailFile'),100,200)
# Select a Kafka publish processor available in the installed NiFi 1.x build.
try:ktype=type_for('.PublishKafka_2_6')
except RuntimeError:ktype=type_for('.PublishKafka')
pub=create('PublishToKafka',ktype,500,200)
# Property names below are for common NiFi 1.x processors; check UI for validation errors.
tail=configure(tail,{'File to Tail':'/shared/application.log','Initial Start Position':'Beginning of File'},['failure'])
pub=configure(pub,{'Kafka Brokers':'kafka:9092','Topic Name':'application-logs'},['success','failure'])
post('/process-groups/'+root+'/connections',{
 'revision':{'version':0},'component':{'name':'Log-to-Kafka','source':{'id':tail['id'],'type':'PROCESSOR','groupId':root},
 'destination':{'id':pub['id'],'type':'PROCESSOR','groupId':root},'selectedRelationships':['success']}})
print('Created NiFi flow. Open http://localhost:8080/nifi, verify processor properties and START both processors.')
