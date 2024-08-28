from flask import Flask, render_template
from dash import Dash, dcc, html, Input, Output
import pandas as pd
import plotly.express as px

#  Flask
server = Flask(__name__)

#  Dash z Flask jako serwerem
app = Dash(__name__, server=server, url_base_pathname='/dashboard/')
data = pd.read_csv('/Users/sebastianwozniczka/Documents/Dashboard_develop/warehouse_data.csv')

# Mapowanie miasta do stanu
city_to_state = {
    'New York': 'NY', 'Houston': 'TX', 'Chicago': 'IL', 'Los Angeles': 'CA', 'Phoenix': 'AZ', 
    'Philadelphia': 'PA', 'San Antonio': 'TX', 'San Diego': 'CA', 'Dallas': 'TX', 'San Jose': 'CA', 
    'Austin': 'TX', 'Jacksonville': 'FL', 'Fort Worth': 'TX', 'Columbus': 'OH', 'Indianapolis': 'IN', 
    'Charlotte': 'NC', 'San Francisco': 'CA', 'Seattle': 'WA', 'Denver': 'CO', 'Washington': 'DC', 
    'Boston': 'MA', 'El Paso': 'TX', 'Nashville': 'TN', 'Detroit': 'MI', 'Oklahoma City': 'OK', 
    'Portland': 'OR', 'Las Vegas': 'NV', 'Memphis': 'TN', 'Louisville': 'KY', 'Baltimore': 'MD', 
    'Milwaukee': 'WI', 'Albuquerque': 'NM', 'Tucson': 'AZ', 'Fresno': 'CA', 'Sacramento': 'CA', 
    'Kansas City': 'MO', 'Long Beach': 'CA', 'Mesa': 'AZ', 'Atlanta': 'GA', 'Colorado Springs': 'CO', 
    'Virginia Beach': 'VA', 'Raleigh': 'NC', 'Omaha': 'NE', 'Miami': 'FL', 'Oakland': 'CA', 
    'Los Angeles': 'CA', 'San Francisco': 'CA', 'Seattle': 'WA', 'Denver': 'CO', 
    'Miami': 'FL', 'Atlanta': 'GA', 'Boston': 'MA', 'Detroit': 'MI', 'Las Vegas': 'NV', 
    'Phoenix': 'AZ', 'Philadelphia': 'PA', 'Dallas': 'TX', 'Orlando': 'FL', 'Austin': 'TX',
    'Charlotte': 'NC', 'San Diego': 'CA', 'Portland': 'OR', 'New Orleans': 'LA', 'Nashville': 'TN',
    'Indianapolis': 'IN', 'Kansas City': 'MO', 'Omaha': 'NE', 'Cincinnati': 'OH', 'Louisville': 'KY', 
    'Milwaukee': 'WI', 'Minneapolis': 'MN', 'St. Louis': 'MO', 'Salt Lake City': 'UT', 
    'Richmond': 'VA', 'Hartford': 'CT', 'Providence': 'RI', 'Birmingham': 'AL', 'Raleigh': 'NC', 
    'Buffalo': 'NY', 'Jacksonville': 'FL', 'Cleveland': 'OH', 'Pittsburgh': 'PA', 'Boise': 'ID', 
    'Anchorage': 'AK', 'Honolulu': 'HI', 'Tulsa': 'OK', 'Des Moines': 'IA', 'Little Rock': 'AR',
    'Dover': 'DE', 'Helena': 'MT', 'Lincoln': 'NE', 'Concord': 'NH', 'Trenton': 'NJ', 
    'Santa Fe': 'NM', 'Bismarck': 'ND', 'Columbus': 'OH', 'Salem': 'OR', 'Columbia': 'SC', 
    'Sioux Falls': 'SD', 'Montpelier': 'VT', 'Charleston': 'WV', 'Cheyenne': 'WY'
}

# Mapowanie stanu do danych
data['State'] = data['Location'].map(city_to_state)

# Layout Dash
app.layout = html.Div([
    dcc.Dropdown(
        id='category-dropdown',
        options=[{'label': category, 'value': category} for category in data['Category'].unique()],
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
    filtered_df = data[data['Category'] == selected_category]
    state_data = filtered_df.groupby('State').agg({
        'Stock Value (USD)': 'sum',
        'Quantity in Stock': 'sum'
    }).reset_index()
    fig = px.choropleth(state_data, locations='State', locationmode='USA-states', color='Stock Value (USD)',
                        scope='usa', title=f'Stock Value of {selected_category} by State')
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
    
    filtered_df = data[data['State'] == state]
    fig = px.scatter_mapbox(filtered_df, lat='Latitude', lon='Longitude', 
                            hover_name='Location', hover_data=['Stock Value (USD)', 'Quantity in Stock'],
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
    
    filtered_df = data[(data['Category'] == selected_category) & (data['Location'] == city)]
    recent_data = filtered_df.tail(days)
    recent_data = recent_data.reset_index()
    recent_data['Days Ago'] = recent_data.index + 1
    line_chart = px.line(recent_data, x='Days Ago', y='Stock Level (units)',
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
    
    filtered_df = data[(data['Category'] == selected_category) & (data['Location'] == city)]
    recent_data = filtered_df.tail(days)
    recent_data = recent_data.reset_index()
    recent_data['Days Ago'] = recent_data.index + 1
    line_chart = px.line(recent_data, x='Days Ago', y='Daily Sales (units)',
                         title=f'Daily Sales of {selected_category} in {city} in Last {days} Days')
    return line_chart

@app.callback(
    Output('pie-chart', 'figure'),
    Input('category-dropdown', 'value')
)
def update_pie_chart(selected_category):
    pie_data = data.groupby('Category').agg({'Stock Value (USD)': 'sum'}).reset_index()
    fig = px.pie(pie_data, values='Stock Value (USD)', names='Category', title='Stock Value by Category')
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
    
    filtered_df = data[data['Category'] == selected_category]
    fig = px.histogram(filtered_df, x='Shelf Life (days)', nbins=20, title=f'Distribution of Shelf Life (days) for {selected_category}')
    return fig

# Flask
@server.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    server.run(debug=True, port=8251)
