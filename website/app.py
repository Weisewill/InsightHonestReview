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
reddit_comments = pd.read_sql_query("SELECT * FROM reddit_comments LIMIT 3;", conn)

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
reddit_comments = reddit_comments[['name', 'body', 'word_count', 'score']]

options = createDropdownOptions(name)

amazon_dt_good = dash_table.DataTable(
    id='amazon_datatable_good',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto'
    },
    columns=[{"name":i, "id":i} for i in amazon_reviews.columns],
    data=amazon_reviews.to_dict('records')
)

amazon_dt_bad = dash_table.DataTable(
    id='amazon_datatable_bad',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto'
    },
    columns=[{"name":i, "id":i} for i in amazon_reviews.columns],
    data=amazon_reviews.to_dict('records')
)

reddit_dt_good = dash_table.DataTable(
    id='reddit_datatable_good',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto'
    },
    columns=[{"name":i, "id":i} for i in reddit_comments.columns],
    data=reddit_comments.to_dict('records')
)

reddit_dt_bad = dash_table.DataTable(
    id='reddit_datatable_bad',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto'
    },
    columns=[{"name":i, "id":i} for i in reddit_comments.columns],
    data=reddit_comments.to_dict('records')
)



app.layout = html.Div([
    dcc.Markdown('''##### Enter video game:'''),
    dcc.Dropdown(
        id='reviews-dropdown',
        options=options,
        value='minecraft'
    ),
    html.Div(id='dd-output-container'),
    dcc.Markdown('''##### Enter min # of words for the review:'''),
    dcc.Input(id="word_count_input", type="number", placeholder="30"),
    html.Div(id='amazon-output-1'),
    html.Div(id='amazon-output-2'),
    dcc.Markdown('''##### Best 3 reviews from Amazon:'''),
    amazon_dt_good,
    dcc.Markdown('''##### Worst 3 reviews from Amazon:'''),
    amazon_dt_bad,
    html.Div(id='reddit-output-1'),
    html.Div(id='reddit-output-2'),
    dcc.Markdown('''##### Best 3 reviews from Reddit:'''),
    reddit_dt_good,
    dcc.Markdown('''##### Worst 3 reviews from Reddit:'''),
    reddit_dt_bad
])

@app.callback(
    [Output(component_id='dd-output-container', component_property='children'),
        Output(component_id='amazon-output-1', component_property='children'),
        Output(component_id='amazon-output-2', component_property='children'),
        Output('amazon_datatable_good', 'data'),
        Output('amazon_datatable_bad', 'data'),
        Output(component_id='reddit-output-1', component_property='children'),
        Output(component_id='reddit-output-2', component_property='children'),
        Output('reddit_datatable_good', 'data'),
        Output('reddit_datatable_bad', 'data')
        ],
    [Input(component_id='reviews-dropdown', component_property='value'),
        Input(component_id='word_count_input', component_property='value')]
)
def updateTable(value, count):
    if count == None:
        count = 30
    amazon_query_good = pd.read_sql_query("SELECT name, review_body, word_count, score FROM amazon_reviews WHERE name = '{}' and word_count >= {} and score > 0.5 ORDER BY score DESC LIMIT 3;".format(value, count), conn)
    amazon_query_bad = pd.read_sql_query("SELECT name, review_body, word_count, score FROM amazon_reviews WHERE name = '{}' and word_count >= {} and score < -0.5 ORDER BY score ASC LIMIT 3;".format(value, count), conn)
    amazon_rev_all = pd.read_sql_query("SELECT COUNT(score) as all FROM amazon_reviews WHERE name = '{}'".format(value), conn)
    amazon_rev_pos = pd.read_sql_query("SELECT COUNT(score) as pos FROM amazon_reviews WHERE name = '{}' and score >= 0".format(value), conn)   

    reddit_query_good = pd.read_sql_query("SELECT name, body, word_count, score FROM reddit_comments WHERE name = '{}' and word_count >= {}  ORDER BY score DESC LIMIT 3;".format(value, count), conn)
    reddit_query_bad = pd.read_sql_query("SELECT name, body, word_count, score FROM reddit_comments WHERE name = '{}' and word_count >= {} ORDER BY score ASC LIMIT 3;".format(value, count), conn)
    reddit_rev_all = pd.read_sql_query("SELECT COUNT(score) as all FROM reddit_comments WHERE name = '{}'".format(value), conn)
    reddit_rev_pos = pd.read_sql_query("SELECT COUNT(score) as pos FROM reddit_comments WHERE name = '{}' and score >= 0".format(value), conn)

    return ('You\'ve entered "{}"'.format(value), 
            '{} reviews from Amazon.'.format(amazon_rev_all['all'][0]),
            '{} reviews are positive. ({})%'.format(int(amazon_rev_pos['pos'][0]), 0 if amazon_rev_all['all'][0] == 0 else round(100*(float(amazon_rev_pos['pos'][0])/amazon_rev_all['all'][0] ), 0)),
        amazon_query_good.to_dict('records'),
        amazon_query_bad.to_dict('records'),
            '{} reviews from Reddit.'.format(reddit_rev_all['all'][0]),
            '{} reviews are positive. ({})%'.format(int(reddit_rev_pos['pos'][0]), 0 if reddit_rev_all['all'][0] == 0 else round(100*(float(reddit_rev_pos['pos'][0])/reddit_rev_all['all'][0] ), 0)),
        reddit_query_good.to_dict('records'),
        reddit_query_bad.to_dict('records')
        )
    
if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
