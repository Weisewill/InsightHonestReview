# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import os
from dash.dependencies import Input, Output
import psycopg2
from flask import render_template
import dash_table
import pandas as pd
from config import (user, password, dbname, host, port)

### BEGIN FUNCTIONS ###

def ping_postgres_db(db_name):
    '''Tests whether the given PostgreSQL database is live.'''
    try:
        conn = psycopg2.connect(host = host, port=port, user = user, dbname = db_name, password = password, sslmode='require')
        conn.close()
        return True
    except:
        return False

def table_exists(table_name):
    sql_query = "SELECT EXISTS (SELECT 1 FROM "+ table_name + ");"
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(sql_query)
        return True
    except:
        return False

def createDropdownOptions(op):
    options = []
    for item in op:
        newOp = {"label":item[0], "value": item[0]}
        options.append(newOp)
    return options

### END FUNCTIONS ###

print("Waiting for connection to model-test database")

connected_to_db = False
while not connected_to_db:
    connected_to_db = ping_postgres_db(dbname)
print("Connected to database")

conn = psycopg2.connect(host = host, port=port, user = user, dbname = dbname, password = password, sslmode='require')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

cur = conn.cursor()
cur.execute("SELECT * FROM amazon_reviews LIMIT 3;")
amazon_reviews = pd.read_sql_query("SELECT * FROM amazon_reviews LIMIT 3;", conn)

cur.execute("SELECT distinct(Name) FROM amazon_reviews ")
name = cur.fetchall()
#print(amazon_review)

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

app.layout = html.Div([
    dcc.Input(id='my-id', value='initial value', type='text'),
    html.Div(id='my-div')
])

amazon_reviews = amazon_reviews[['name', 'review_body', 'word_count', 'score']]

options = createDropdownOptions(name)

dt_good = dash_table.DataTable(
    id='datatable_good',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto'
    },
    columns=[{"name":i, "id":i} for i in amazon_reviews.columns],
    data=amazon_reviews.to_dict('records')
)

dt_bad = dash_table.DataTable(
    id='datatable_bad',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto'
    },
    columns=[{"name":i, "id":i} for i in amazon_reviews.columns],
    data=amazon_reviews.to_dict('records')
)


app.layout = html.Div([
    dcc.Markdown('''
##### Enter video game:
'''),
    dcc.Dropdown(
        id='reviews-dropdown',
        options=options,
        value='Bloodborne'
    ),
    html.Div(id='dd-output-container'),
    dcc.Markdown('''
##### Enter min # of words for the review:
'''),
    dcc.Input(
        id="word_count_input", type="number", placeholder="30"
    ),
    dcc.Markdown('''
##### Best 3 reviews:
'''),
    dt_good,
    dcc.Markdown('''
##### Worst 3 reviews:
'''),
    dt_bad
])

@app.callback(
    [Output(component_id='dd-output-container', component_property='children'),
        Output('datatable_good', 'data'),
        Output('datatable_bad', 'data')],
    [Input(component_id='reviews-dropdown', component_property='value'),
        Input(component_id='word_count_input', component_property='value')]
)
def updateTable(value, count):
    if count == None:
        count = 30
    new_query_good = pd.read_sql_query("SELECT name, review_body, word_count, score FROM amazon_reviews WHERE name = '{}' and word_count >= {} and score > 0.5 ORDER BY score DESC LIMIT 3;".format(value, count), conn)
    new_query_bad = pd.read_sql_query("SELECT name, review_body, word_count, score FROM amazon_reviews WHERE name = '{}' and word_count >= {} and score < -0.5 ORDER BY score ASC LIMIT 3;".format(value, count), conn)

    return ('You\'ve entered "{}"'.format(value), 
        new_query_good.to_dict('records'),
        new_query_bad.to_dict('records')
        )
    
if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
