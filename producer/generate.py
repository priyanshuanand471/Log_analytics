"""Generate synthetic common-log-style JSON records; choose Kafka or file mode."""
import os,random,json,time,uuid,argparse
from datetime import datetime,timezone

def event():
    level=random.choices(['INFO','WARN','ERROR'],[75,15,10])[0]
    return {'event_id':str(uuid.uuid4()),'event_time':datetime.now(timezone.utc).isoformat(),
            'service':random.choice(['Auth','Payment','Orders','Gateway']),
            'level':level,'message':random.choice({'INFO':['Request complete','User logged in'],
              'WARN':['Slow API response','Retry scheduled'],
              'ERROR':['Database timeout','Request failed']}[level]),
            'host':f'app-{random.randint(1,3)}'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--mode',choices=['kafka','file'],default='kafka')
    p.add_argument('--count',type=int,default=0);p.add_argument('--interval',type=float,default=0.3)
    a=p.parse_args(); producer=None
    if a.mode=='kafka':
        from kafka import KafkaProducer
        producer=KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP','localhost:9092'),
           value_serializer=lambda x:json.dumps(x).encode(),acks='all')
    count=0
    try:
        while not a.count or count<a.count:
            record=event()
            if producer:
                producer.send('application-logs',record,key=record['service'].encode()).get(timeout=15)
            else:
                path=os.getenv('LOG_FILE','logs/application.log')
                os.makedirs(os.path.dirname(path),exist_ok=True)
                with open(path,'a',encoding='utf8') as f:f.write(json.dumps(record)+'\n')
            print(json.dumps(record),flush=True)
            count+=1;time.sleep(a.interval)
    except KeyboardInterrupt:pass
    finally:
        if producer:producer.flush();producer.close()
