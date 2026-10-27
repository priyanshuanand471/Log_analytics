"""Dash/Plotly UI for live micro-batch metrics and cold-path summaries."""
import os
from collections import defaultdict
from cassandra.cluster import Cluster
from dash import Dash,html,dcc,Input,Output
import plotly.graph_objects as go

app=Dash(__name__)
app.title='Log Analytics | Kafka + Spark'
app.layout=html.Div(style={'fontFamily':'Arial,sans-serif','maxWidth':'1100px','margin':'auto','padding':'28px'},children=[
 html.H1('Real-Time Log Analytics'),
 html.P('Kafka • Spark Structured Streaming • Cassandra • HDFS • NiFi'),
 html.Div(id='status',style={'marginBottom':'12px'}),
 dcc.Graph(id='levels'),dcc.Graph(id='services'),
 html.H3('Historical daily summary (Spark batch layer)'),
 dcc.Graph(id='history'),dcc.Interval(id='refresh',interval=10000,n_intervals=0)
])

def fetch():
    cluster=Cluster([os.getenv('CASSANDRA_HOST','cassandra')],connect_timeout=5)
    try:
        session=cluster.connect('log_analytics')
        # Demo-only ALLOW FILTERING-free full table scans; use purpose-built
        # materialized/query tables and pagination for production datasets.
        live=list(session.execute('SELECT bucket,service,level,count FROM metrics_by_window LIMIT 1000'))
        daily=list(session.execute('SELECT day,service,level,count FROM batch_summary LIMIT 1000'))
        return live,daily
    finally:cluster.shutdown()

def chart(rows,field,title):
    values=defaultdict(int)
    for row in rows:values[getattr(row,field)]+=int(row.count)
    fig=go.Figure([go.Bar(x=list(values),y=list(values.values()))])
    fig.update_layout(title=title,template='plotly_white',margin=dict(l=30,r=30,t=50,b=30))
    return fig

@app.callback(Output('status','children'),Output('levels','figure'),Output('services','figure'),Output('history','figure'),Input('refresh','n_intervals'))
def refresh(_):
    try:
        live,daily=fetch()
        return (f'Connected • {len(live)} live metric groups • {len(daily)} historical groups',
                chart(live,'level','Live counts by severity (demo batches)'),
                chart(live,'service','Live counts by service'),
                chart(daily,'day','Historical counts by day'))
    except Exception as exc:
        blank=go.Figure();blank.update_layout(template='plotly_white')
        return f'Waiting for Cassandra/schema: {exc}',blank,blank,blank

if __name__=='__main__':app.run_server(host='0.0.0.0',port=8050,debug=False)
