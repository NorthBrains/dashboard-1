from flask import Flask, render_template
from dash import Dash, dcc, html, Input, Output
import json
import plotly.express as px

#Cassandra connector for warehouse_data
from libs.warehouse_data import continuous_fetch_warehouse

server = Flask(__name__)

app = Dash(__name__, server=server, url_base_pathname='/dashboard/warehouse/')

#fetch data from Cassandra
df_generator = continuous_fetch_warehouse()
data = next(df_generator)

#Map city to state from json file
with open('templates/city_state.json', 'r') as file:
    city_to_state = json.load(file)

data['state'] = data['location'].map(city_to_state)

#Layout Dash
app.layout = html.Div([
    dcc.Dropdown(
        id='category-dropdown',
        options=[{'label': category, 'value': category} for category in data['category'].unique()],
        value='Electronics'
    ),
    html.Div([
        dcc.Graph(id='location-map', style={'height': '600px'})
    ]),
    html.Div([
        html.Label('Select number of days for Stock Level Line Chart:'),
        dcc.Slider(
            id='stock-days-slider',
            min=1,
            max=30,
            value=10,
            marks={i: str(i) for i in range(1, 31)},
            step=1
        ),
        dcc.Graph(id='city-map', style={'height': '600px'})
    ]),
    html.Div([
        dcc.Graph(id='stock-level-line-chart')
    ]),
    html.Div([
        html.Label('Select number of days for Daily Sales Line Chart:'),
        dcc.Slider(
            id='sales-days-slider',
            min=1,
            max=30,
            value=10,
            marks={i: str(i) for i in range(1, 31)},
            step=1
        ),
        dcc.Graph(id='daily-sales-line-chart')
    ]),
    html.Div([
        dcc.Graph(id='pie-chart'),
        dcc.Graph(id='histogram')
    ]),
])

# aktualizacje wykresów
@app.callback(
    Output('location-map', 'figure'),
    Input('category-dropdown', 'value')
)

def update_location_map(selected_category):
    filtered_df = data[data['category'] == selected_category]
    state_data = filtered_df.groupby('state').agg({
        'stock_value': 'sum',
        'quantity_in_stock': 'sum'
    }).reset_index()
    fig = px.choropleth(state_data, locations='state', locationmode='USA-states', color='stock_value',
                        scope='usa', title=f'Stock Value of {selected_category} by state')
    fig.update_layout(height=600)
    return fig

@app.callback(
    Output('city-map', 'figure'),
    Input('location-map', 'clickData')
)
def update_city_map(clickData):
    if clickData is None or 'points' not in clickData or len(clickData['points']) == 0:
        state = 'TX'  # Domyślnie texas
    else:
        state = clickData['points'][0]['location']
    
    filtered_df = data[data['state'] == state]
    fig = px.scatter_mapbox(filtered_df, lat='latitude', lon='longitude', 
                            hover_name='location', hover_data=['stock_value', 'quantity_in_stock'],
                            zoom=5, height=600)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(title=f'Stock Levels by City in {state}')
    return fig

@app.callback(
    Output('stock-level-line-chart', 'figure'),
    [Input('category-dropdown', 'value'),
     Input('city-map', 'clickData'),
     Input('stock-days-slider', 'value')]
)
def update_stock_level_line_chart(selected_category, city_click_data, days):
    if city_click_data is None or 'points' not in city_click_data or len(city_click_data['points']) == 0:
        city = 'New York'
        selected_category = 'Footwear'
    else:
        city = city_click_data['points'][0]['hovertext']
    
    filtered_df = data[(data['category'] == selected_category) & (data['location'] == city)]
    recent_data = filtered_df.tail(days)
    recent_data = recent_data.reset_index()
    recent_data['days_ago'] = recent_data.index + 1
    line_chart = px.line(recent_data, x='days_ago', y='stock_level',
                         title=f'Stock Level of {selected_category} in {city} in Last {days} Days')
    return line_chart

@app.callback(
    Output('daily-sales-line-chart', 'figure'),
    [Input('category-dropdown', 'value'),
     Input('city-map', 'clickData'),
     Input('sales-days-slider', 'value')]
)
def update_daily_sales_line_chart(selected_category, city_click_data, days):
    if city_click_data is None or 'points' not in city_click_data or len(city_click_data['points']) == 0:
        city = 'New York'
        selected_category = 'Footwear'
    else:
        city = city_click_data['points'][0]['hovertext']
    
    filtered_df = data[(data['category'] == selected_category) & (data['location'] == city)]
    recent_data = filtered_df.tail(days)
    recent_data = recent_data.reset_index()
    recent_data['days_ago'] = recent_data.index + 1
    line_chart = px.line(recent_data, x='days_ago', y='daily_sales',
                         title=f'Daily Sales of {selected_category} in {city} in Last {days} Days')
    return line_chart

@app.callback(
    Output('pie-chart', 'figure'),
    Input('category-dropdown', 'value')
)
def update_pie_chart(selected_category):
    pie_data = data.groupby('category').agg({'stock_value': 'sum'}).reset_index()
    fig = px.pie(pie_data, values='stock_value', names='category', title='Stock Value by category')
    return fig

@app.callback(
    Output('histogram', 'figure'),
    Input('pie-chart', 'clickData')
)
def update_histogram(clickData):
    if clickData is None or 'points' not in clickData or len(clickData['points']) == 0:
        selected_category = 'Footwear'
    else:
        selected_category = clickData['points'][0]['label']
    
    filtered_df = data[data['category'] == selected_category]
    fig = px.histogram(filtered_df, x='shelf_life', nbins=20, title=f'Distribution of shelf_life for {selected_category}')
    return fig

# Flask
@server.route('/')
def index():
    return render_template('index_warehouse.html')

if __name__ == '__main__':
    server.run(debug=True, port=5000)
