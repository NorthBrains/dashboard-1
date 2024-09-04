import plotly.express as px
from dash import Dash, dcc, html, Input, Output
from flask import Flask, render_template
import json

#Cassandra connector for sales_data
from libs.sales_data import continuous_fetch_sales

server = Flask(__name__)

app = Dash(__name__, server=server, url_base_pathname='/dashboard/sales')

#fetch data from Cassandra
data_generator = continuous_fetch_sales()
data = next(data_generator)

#Map city to state from json file
with open('templates/city_state.json', 'r') as file:
    city_to_state = json.load(file)

data['state'] = data['location'].map(city_to_state)

# Layout aplikacji
app.layout = html.Div([
    dcc.Dropdown(
        id='category-dropdown',
        options=[{'label': category, 'value': category} for category in data['category'].unique()],
        value='Clothing'
    ),
    dcc.Graph(id='us-map'),
    dcc.Graph(id='bar-chart'),
    dcc.Graph(id='line-chart'),
    dcc.Slider(
        id='days-slider',
        min=1,
        max=30,
        value=10,
        marks={i: str(i) for i in range(1, 31)},
        step=1
    ),
    html.Button('Toggle to Units', id='toggle-button', n_clicks=0),
    dcc.Graph(id='category-pie-chart'),
    dcc.Graph(id='subcategory-histogram'),
    dcc.Graph(id='payment-method-pie-chart'),
    dcc.Slider(
        id='payments-slider',
        min=1,
        max=50,
        value=10,
        marks={i: str(i) for i in range(1, 51)},
        step=1
    ),
    dcc.Graph(id='payment-method-line-chart')
])

@app.callback(
    Output('us-map', 'figure'),
    Input('category-dropdown', 'value')
)
def update_us_map(selected_category):
    filtered_data = data[data['category'] == selected_category]
    grouped_data = filtered_data.groupby('location')['purchase_amount'].sum().reset_index()
    fig = px.choropleth(data_frame=grouped_data, locations='location', locationmode='USA-states', color='purchase_amount',
                        scope='usa', title=f'Sales of {selected_category} by State')
    return fig

# wykres slupkowy (domyslnie texas)
@app.callback(
    Output('bar-chart', 'figure'),
    Input('us-map', 'clickData')
)
def update_bar_chart(clickData):
    if clickData is None:
        state = 'TX'  
    else:
        state = clickData['points'][0]['location']
    
    filtered_data = data[data['location'] == state]
    grouped_data = filtered_data.groupby('category')['purchase_amount'].sum().reset_index()
    bar_chart = px.bar(grouped_data, x='category', y='purchase_amount',
                       title=f'Sales by category in {state}')
    return bar_chart

# wykres liniowy
@app.callback(
    Output('line-chart', 'figure'),
    [Input('category-dropdown', 'value'),
     Input('us-map', 'clickData'),
     Input('days-slider', 'value'),
     Input('toggle-button', 'n_clicks')]
)
def update_line_chart(selected_category, clickData, days, n_clicks):
    if clickData is None:
        state = 'TX'  
    else:
        state = clickData['points'][0]['location']
    
    filtered_data = data[(data['category'] == selected_category) & (data['location'] == state)]
    recent_sales = filtered_data.tail(days)
    recent_sales = recent_sales.reset_index()
    recent_sales['days_ago'] = recent_sales.index + 1
    
    if n_clicks % 2 == 0:
        line_chart = px.line(recent_sales, x='days_ago', y='purchase_amount',
                             title=f'Last {days} Sales of {selected_category} in {state}')
    else:
        line_chart = px.line(recent_sales, x='days_ago', y='quantity_sold',
                             title=f'Last {days} Sales (Units) of {selected_category} in {state}')
    
    return line_chart

# wykres kołowy z rozkładem % sprzedaży per kategoria w stanie
@app.callback(
    Output('category-pie-chart', 'figure'),
    Input('us-map', 'clickData')
)
def update_category_pie_chart(clickData):
    if clickData is None:
        return px.pie(title='Select a state to see category distribution')
    
    state = clickData['points'][0]['location']
    filtered_data = data[data['location'] == state]
    grouped_data = filtered_data.groupby('category')['purchase_amount'].sum().reset_index()
    total_amount = grouped_data['purchase_amount'].sum()
    grouped_data['percentage'] = (grouped_data['purchase_amount'] / total_amount) * 100
    pie_chart = px.pie(grouped_data, names='category', values='percentage',
                       title=f'Sales percentage by category in {state}')
    return pie_chart

# histogram z podkategoriami danej kategorii
@app.callback(
    Output('subcategory-histogram', 'figure'),
    [Input('category-dropdown', 'value'),
     Input('us-map', 'clickData')]
)
def update_subcategory_histogram(selected_category, clickData):
    if clickData is None:
        state = 'TX'  
    else:
        state = clickData['points'][0]['location']
    
    filtered_data = data[(data['category'] == selected_category) & (data['location'] == state)]
    grouped_data = filtered_data.groupby('Item Purchased')['purchase_amount'].sum().reset_index()
    histogram = px.histogram(grouped_data, x='purchase_amount', y='Item Purchased', orientation='h',
                             title=f'Sales by Subcategory in {state}')
    return histogram

#wykres kolowy rodzaje platnosci
@app.callback(
    Output('payment-method-pie-chart', 'figure'),
    Input('category-dropdown', 'value')
)
def update_payment_method_pie_chart(selected_category):
    pie_chart = px.pie(data, names='payment_method', values='purchase_amount',
                       title='Sales Distribution by payment_method')
    return pie_chart

# liniowy rodzaje platnosci (domyslnie credit card)
@app.callback(
    Output('payment-method-line-chart', 'figure'),
    [Input('payment-method-pie-chart', 'clickData'),
     Input('payments-slider', 'value')]
)
def update_payment_method_line_chart(clickData, payments):
    if clickData is None:
        payment_method = 'Credit Card'  
    else:
        payment_method = clickData['points'][0]['label']
    
    filtered_data = data[data['payment_method'] == payment_method]
    recent_payments = filtered_data.tail(payments)
    recent_payments = recent_payments.reset_index()
    recent_payments['days_ago'] = recent_payments.index + 1
    line_chart = px.line(recent_payments, x='days_ago', y='purchase_amount',
                         title=f'Last {payments} Payments by {payment_method}')
    return line_chart


# Flask
@server.route('/')
def index():
    return render_template('index_sales.html')

if __name__ == '__main__':
    server.run(debug=True, port=8260)