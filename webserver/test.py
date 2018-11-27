#!/usr/bin/env python2.7

"""
Columbia W4111 Intro to databases
Example webserver

To run locally

python server.py

Go to http://localhost:8111 in your browser


A debugger such as "pdb" may be helpful for debugging.
Read about it online.
"""

import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response
import folium
from flask_paginate import Pagination, get_page_args
from collections import namedtuple
import numpy as np
import json

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)


# XXX: The Database URI should be in the format of:
#
#     postgresql://USER:PASSWORD@<IP_OF_POSTGRE_SQL_SERVER>/<DB_NAME>
#
# For example, if you had username ewu2493, password foobar, then the following line would be:
#
#     DATABASEURI = "postgresql://ewu2493:foobar@<IP_OF_POSTGRE_SQL_SERVER>/postgres"
#
# For your convenience, we already set it to the class database

# Use the DB credentials you received by e-mail
DB_USER = "yy2792"
DB_PASSWORD = "palhm70w"

DB_SERVER = "w4111.cisxo09blonu.us-east-1.rds.amazonaws.com"

DATABASEURI = "postgresql://"+DB_USER+":"+DB_PASSWORD+"@"+DB_SERVER+"/w4111"


#
# This line creates a database engine that knows how to connect to the URI above
#
engine = create_engine(DATABASEURI)


# Here we create a test table and insert some values in it
engine.execute("""DROP TABLE IF EXISTS test;""")

engine.execute("""CREATE TABLE IF NOT EXISTS test (
id serial,
name text
);""")
engine.execute("""INSERT INTO test(name) VALUES ('grace hopper'), ('alan turing'), ('ada lovelace');""")


@app.before_request
def before_request():
  """
  This function is run at the beginning of every web request
  (every time you enter an address in the web browser).
  We use it to setup a database connection that can be used throughout the request

  The variable g is globally accessible
  """
  try:
    g.conn = engine.connect()
  except:
    print "uh oh, problem connecting to database"
    import traceback; traceback.print_exc()
    g.conn = None

@app.teardown_request
def teardown_request(exception):
  """
  At the end of the web request, this makes sure to close the database connection.
  If you don't the database could run out of memory!
  """
  try:
    g.conn.close()
  except Exception as e:
    pass


#
# @app.route is a decorator around index() that means:
#   run index() whenever the user tries to access the "/" path using a GET request
#
# If you wanted the user to go to e.g., localhost:8111/foobar/ with POST or GET then you could use
#
#       @app.route("/foobar/", methods=["POST", "GET"])
#
# PROTIP: (the trailing / in the path is important)
#
# see for routing: http://flask.pocoo.org/docs/0.10/quickstart/#routing
# see for decorators: http://simeonfranklin.com/blog/2012/jul/1/python-decorators-in-12-steps/
#
@app.route('/')
def index():
  """
  request is a special object that Flask provides to access web request information:

  request.method:   "GET" or "POST"
  request.form:     if the browser submitted a form, this contains the data in the form
  request.args:     dictionary of URL arguments e.g., {a:1, b:2} for http://localhost?a=1&b=2

  See its API: http://flask.pocoo.org/docs/0.10/api/#incoming-request-data
  """

  # DEBUG: this is debugging code to see what request looks like
  print request.args


  #
  # example of a database query
  #
  cursor = g.conn.execute("SELECT name FROM test")
  names = []
  for result in cursor:
    names.append(result['name'])  # can also be accessed using result[0]
  cursor.close()

  #
  # Flask uses Jinja templates, which is an extension to HTML where you can
  # pass data to a template and dynamically generate HTML based on the data
  # (you can think of it as simple PHP)
  # documentation: https://realpython.com/blog/python/primer-on-jinja-templating/
  #
  # You can see an example template in templates/index.html
  #
  # context are the variables that are passed to the template.
  # for example, "data" key in the context variable defined below will be
  # accessible as a variable in index.html:
  #
  #     # will print: [u'grace hopper', u'alan turing', u'ada lovelace']
  #     <div>{{data}}</div>
  #
  #     # creates a <div> tag for each element in data
  #     # will print:
  #     #
  #     #   <div>grace hopper</div>
  #     #   <div>alan turing</div>
  #     #   <div>ada lovelace</div>
  #     #
  #     {% for n in data %}
  #     <div>{{n}}</div>
  #     {% endfor %}
  #
  context = dict(data = names)

  #
  # render_template looks in the templates/ folder for files.
  # for example, the below file reads template/index.html
  #
  return render_template("index.html", **context)

#
# This is an example of a different path.  You can see it at
#
#     localhost:8111/another
#
# notice that the functio name is another() rather than index()
# the functions for each app.route needs to have different names
#
@app.route('/another')
def another():
  return render_template("anotherfile.html")


# Example of adding new data to the database
@app.route('/add', methods=['POST'])
def add():
  name = request.form['name']
  print name
  cmd = 'INSERT INTO test(name) VALUES (:name1), (:name2)';
  g.conn.execute(text(cmd), name1 = name, name2 = name);
  return redirect('/')

# parent directory for all station data
@app.route('/station')
def station():
    return render_template("station.html")

@app.route('/addStation', methods=['POST'])
def addStation():
  sid = request.form['sid']
  name = request.form['name']
  latitude = request.form['latitude']
  longtitude = request.form['longtitude']
  cmd = "INSERT INTO station(sid, name, latitude, longtitude) VALUES (:sid, :name, :latitude, :longtitude)"
  g.conn.execute(text(cmd), sid = sid, name = name, latitude = latitude, longtitude = longtitude)
  return redirect('/stationData')

@app.route('/stationData')
def stationData():

    cursor = g.conn.execute("SELECT * FROM station")
    items = []

    for result in cursor:
        an_item = dict(sid = result['sid'], name = result['name'], latitude = result['latitude'],
                       longtitude = result['longtitude'])
        items.append(an_item)

    cursor.close()

    return render_template("stationData.html", items = items)

@app.route('/stationMap')
def stationMap():

  cursor = g.conn.execute("SELECT * FROM station")
  items = []

  for result in cursor:
    an_item = dict(sid=result['sid'], name=result['name'], latitude=result['latitude'],
                   longtitude=result['longtitude'])
    items.append(an_item)

  cursor.close()

  # make map
  folium_map = folium.Map(location=[40.738, -73.98],
                          zoom_start=13,
                          tiles="CartoDB dark_matter")

  color = "#E37222"

  for item in items:

    popup_text = """{}<br>
                latitude: {}<br> 
                longtitude: {}"""

    popup_text = popup_text.format(item['name'],
                                   item['latitude'],
                                   item['longtitude'])

    marker = folium.CircleMarker(location=[item['latitude'], item['longtitude']], color=color,
                                 fill=True, popup=popup_text)
    marker.add_to(folium_map)

  folium_map.save(outfile='./templates/stationMap.html')

  return render_template('stationMap.html')

@app.route('/stationTrips')
def stationTrips():
    cursor = g.conn.execute('''
    SELECT s.sid, s.name, s.latitude, s.longtitude, b.trips FROM station s,
    (select s2.sid, count(rid) trips from station s2, involves
    where s2.sid = involves.sid 
    group by s2.sid) b
    where s.sid = b.sid
    ''')

    items = []

    for result in cursor:
        an_item = dict(sid=result['sid'], name=result['name'], latitude=result['latitude'],
                       longtitude=result['longtitude'], trips = result['trips'])
        items.append(an_item)

    cursor.close()

    # make map
    folium_map = folium.Map(location=[40.738, -73.98],
                            zoom_start=13,
                            tiles="CartoDB dark_matter")

    for item in items:

        color = "#E37222"
        if item['trips'] <= 10:
            color = "#0A8A9F"

        radius = item['trips'] / 2000

        popup_text = """{}<br>
                latitude: {}<br> 
                longtitude: {}<br>
                trips: {}"""

        popup_text = popup_text.format(item['name'],
                                       item['latitude'],
                                       item['longtitude'],
                                       item['trips'])

        marker = folium.CircleMarker(location=[item['latitude'], item['longtitude']], radius = radius, color=color,
                                     fill=True, popup=popup_text)
        marker.add_to(folium_map)

    folium_map.save(outfile='./templates/stationTrips.html')

    return render_template('stationTrips.html')

@app.route('/stationOutflow')
def stationOutflow():
    cursor = g.conn.execute('''
    with station_gap as(select station.sid,
    SUM(CASE WHEN arrive_depart = TRUE THEN 1 ELSE 0 END) AS count_arr,
    SUM(CASE WHEN arrive_depart = FALSE THEN 1 ELSE 0 END) AS count_depart
    from involves, station where station.sid = involves.sid
    group by station.sid)
    select s.sid, s.name, s.latitude, s.longtitude, (count_arr - count_depart) as diff
    from station_gap, station s
    where s.sid = station_gap.sid
    ''')

    items = []

    for result in cursor:
        an_item = dict(sid=result['sid'], name=result['name'], latitude=result['latitude'],
                       longtitude=result['longtitude'], diff = result['diff'])
        items.append(an_item)

    cursor.close()

    # make map
    folium_map = folium.Map(location=[40.738, -73.98],
                            zoom_start=13,
                            tiles="CartoDB dark_matter")

    for item in items:

        color = "#E37222"
        if item['diff'] < 0:
            color = "#0A8A9F"

        radius = abs(item['diff']) / 20

        popup_text = """{}<br>
                latitude: {}<br> 
                longtitude: {}<br>
                trips: {}"""

        popup_text = popup_text.format(item['name'],
                                       item['latitude'],
                                       item['longtitude'],
                                       item['diff'])

        marker = folium.CircleMarker(location=[item['latitude'], item['longtitude']], radius = radius, color=color,
                                     fill=True, popup=popup_text)
        marker.add_to(folium_map)

    folium_map.save(outfile='./templates/stationOutflow.html')

    return render_template('stationOutflow.html')

@app.route('/stationRoutes')
def stationRoutes():
    cursor = g.conn.execute('''
    with station_arrive as  (select i2.rid, i2.sid, i2.time, s2.name, s2.latitude, s2.longtitude
    From involves i2, station s2
    Where i2.sid = s2.sid and i2.arrive_depart = True),
    station_depart as  (select i2.rid, i2.sid, i2.time, s2.name, s2.latitude, s2.longtitude
    From involves i2, station s2
    Where i2.sid = s2.sid and i2.arrive_depart  = False),
   Trips as (Select station_depart.time starttime, station_arrive.time
    stoptime, station_depart.sid start_sid, station_depart.name start_station, 
    station_depart.latitude start_station_latitude, station_depart.longtitude start_station_longtitude, 
    station_arrive.sid stop_sid, station_arrive.name stop_station, station_arrive.latitude stop_station_latitude, 
    station_arrive.longtitude stop_station_longtitude 
    From station_arrive, station_depart, ride
    Where station_arrive.rid = station_depart.rid and station_arrive.rid = ride.rid),
    Routes as (Select start_sid, stop_sid
    From (select start_sid, stop_sid, ROW_NUMBER() OVER (PARTITION BY start_sid ORDER BY freq DESC) as rn 
    From (select start_sid, stop_sid, count(stop_sid) as freq 
    From trips group by start_sid, stop_sid) trips_freq) ranked_trips_freq
    Where rn < 6
    Order by start_sid)
    Select s1.name start_station, s1.latitude start_station_latitude, s1.longtitude start_station_longtitude, 
    s2.name stop_station, s2.latitude stop_station_latitude, s2.longtitude stop_station_longtitude
    From routes, station s1, station s2
    Where routes.start_sid = s1.sid and routes.stop_sid = s2.sid
    ''')

    items = []

    for result in cursor:
        an_item = dict(start_station =result['start_station'], start_station_latitude =result['start_station_latitude'],
                       start_station_longtitude =result['start_station_longtitude'],
                       stop_station =result['stop_station'], stop_station_latitude = result['stop_station_latitude'],
                       stop_station_longtitude = result['stop_station_longtitude'])
        items.append(an_item)

    cursor.close()

    # make map
    folium_map = folium.Map(location=[40.738, -73.98],
                            zoom_start=13,
                            tiles="CartoDB dark_matter")

    for item in items:
        color = "#E37222"

        p1 = [item['start_station_latitude'], item['start_station_longtitude']]
        p2 = [item['stop_station_latitude'], item['stop_station_longtitude']]

        popup_text1 = """{}<br>
                        latitude: {}<br> 
                        longtitude: {}<br>
                      """

        popup_text1 = popup_text1.format(item['start_station'],
                                       item['start_station_latitude'],
                                       item['start_station_longtitude'])

        popup_text2 = """{}<br>
                        latitude: {}<br> 
                        longtitude: {}<br>
                      """

        popup_text2 = popup_text2.format(item['stop_station'],
                                        item['stop_station_latitude'],
                                        item['stop_station_longtitude'])

        folium.Marker(location=p1, icon=folium.Icon(color=color, icon='cloud'), popup=popup_text1).add_to(folium_map)
        folium.Marker(location=p2, icon=folium.Icon(color=color, icon='cloud'), popup=popup_text2).add_to(folium_map)

        folium.PolyLine(locations=[p1, p2], color=color).add_to(folium_map)

        arrow = get_arrows(locations=[p1, p2], some_map = folium_map, n_arrows=1)[0]

        arrow.add_to(folium_map)

    folium_map.save(outfile='./templates/stationRoutes.html')

    return render_template('stationRoutes.html')


# parent directory for trips
@app.route('/trips')
def trips():
    return render_template("trips.html")

@app.route('/tripsData/month/<int:month>/')
def tripsData(month):

    page, per_page, offset = get_page_args(page_parameter='page',
                                           per_page_parameter='per_page')

    per_page = 200
    offset = (page - 1) * per_page

    temp_query = '''
    with station_arrive as  (select i2.rid, i2.sid, i2.time, s2.name, s2.latitude, s2.longtitude
    From involves i2, station s2
    Where i2.sid = s2.sid and i2.arrive_depart = True),
    station_depart as  (select i2.rid, i2.sid, i2.time, s2.name, s2.latitude, s2.longtitude
    From involves i2, station s2
    Where i2.sid = s2.sid and i2.arrive_depart  = False)
    Select station_depart.time starttime, station_arrive.time
    stoptime, age(station_arrive.time, station_depart.time) tripduration, station_depart.name start_station, 
    station_depart.latitude start_station_latitude, station_depart.longtitude start_station_longtitude, 
    station_arrive.name stop_station, station_arrive.latitude stop_station_latitude, 
    station_arrive.longtitude stop_station_longtitude, bike.bid bike_id, users.uid user_id, users.ctype, users.gender,
    users.birthyear
    From station_arrive, station_depart, ride, bike, users
    Where station_arrive.rid = station_depart.rid and station_arrive.rid = ride.rid and bike.bid = ride.bid and users.uid = ride.uid
    and date_part('month', station_depart.time) = {}
    order by starttime
    '''.format(month)

    cursor = g.conn.execute(temp_query)
    items = []

    for result in cursor:
        an_item = dict(starttime = result['starttime'], stoptime = result['stoptime'], tripduration = result['tripduration'],
                       start_station = result['start_station'], start_station_latitude =result['start_station_latitude'],
                       start_station_longtitude = result['start_station_longtitude'], stop_station = result['stop_station'],
                       stop_station_latitude =result['stop_station_latitude'],
                       stop_station_longtitude = result['stop_station_longtitude'], bike_id = result['bike_id'],
                       user_id = result['user_id'], ctype = result['ctype'], gender = result['gender'], birthyear = result['birthyear'])
        items.append(an_item)

    cursor.close()

    total = len(items)

    pagination_users = items[offset:offset + per_page]

    pagination = Pagination(page=page, per_page=per_page, total=total, css_framework='bootstrap4')

    return render_template("tripsData.html", items = pagination_users, pagination = pagination)


@app.route('/addTrips', methods=['POST'])
def addTrips():
    starttime = request.form['starttime']
    stoptime = request.form['stoptime']
    start_station_sid = request.form['start_station_sid']
    stop_station_sid = request.form['stop_station_sid']
    bike_id = request.form['bike_id']
    user_id = request.form['user_id']

    if starttime > stoptime:
        return json.dumps({"error": "starttime should be earlier than stoptime"}), 500

    # only existing station should be accessed
    cmd1 = "select exists(select 1 from station where sid = :sid)"

    # if already exists sid
    cursor = g.conn.execute(text(cmd1), sid = start_station_sid)
    res = None
    for c in cursor:
        res = c
    if not res['exists']:
        return json.dumps({"error": "Invalid sid for start station sid"}), 500

    cursor = g.conn.execute(text(cmd1), sid=stop_station_sid)
    res = None
    for c in cursor:
        res = c
    if not res['exists']:
        return json.dumps({"error": "Invalid sid for stop station sid"}), 500

    # only existing bid should be accessed
    cmd2 = "select exists(select 1 from bike where bid = :bid)"
    cursor = g.conn.execute(text(cmd1), bid=stop_station_sid)
    res = None
    for c in cursor:
        res = c
    if not res['exists']:
        return json.dumps({"error": "Invalid sid for stop station sid"}), 500


# parent directory for all weather data
@app.route('/weather')
def weather():
    return render_template("weather.html")

@app.route('/weatherData')
def weatherData():
    cursor = g.conn.execute("SELECT * FROM weather")
    items = []

    for result in cursor:
        an_item = dict(date_id=result['date_id'], max_temp=result['max_temp'], min_temp=result['min_temp'],
                       avg_temp=result['avg_temp'], hdds=result['hdds'], cdds=result['cdds'],
                       precipitation=result['precipitation'], snowfall=result['snowfall'],
                       snowdepth=result['snowdepth'])
        items.append(an_item)

    cursor.close()

    return render_template("weatherData.html", items=items)

@app.route('/addWeather', methods=['POST'])
def addWeather():
    date_id = request.form['date_id']
    max_temp = request.form['max_temp']
    min_temp = request.form['min_temp']
    avg_temp = request.form['avg_temp']
    hdds = request.form['hdds']
    cdds = request.form['cdds']
    precipitation = request.form['precipitation']
    snowfall = request.form['snowfall']
    snowdepth = request.form['snowdepth']

    cmd = '''
        INSERT INTO weather(date_id, max_temp, min_temp, avg_temp, hdds, cdds, 
        precipitation, snowfall, snowdepth) VALUES (:date_id, :max_temp, :min_temp, 
        :avg_temp, :hdds, :cdds, :precipitation, :snowfall, :snowdepth)
        '''
    g.conn.execute(text(cmd), date_id = date_id, max_temp = max_temp, min_temp = min_temp,
                   avg_temp = avg_temp, hdds = hdds, cdds = cdds,
                   precipitation = precipitation, snowfall = snowfall, snowdepth = snowdepth)
    return redirect('/weatherData')

# parent directory for all station data
@app.route('/users')
def users():
    return render_template("users.html")

@app.route('/usersData')
def usersData():

    cursor = g.conn.execute("SELECT * FROM users")
    items = []

    for result in cursor:
        an_item = dict(uid = result['uid'], ctype = result['ctype'], gender = result['gender'],
                       birthyear = result['birthyear'])
        items.append(an_item)

    cursor.close()

    return render_template("usersData.html", items = items)

@app.route('/addUser', methods=['POST'])
def addUser():
    ctype = request.form['ctype']
    gender = request.form['gender']
    birthyear = request.form['birthyear']
    cmd1 = "Select count(uid) count1 from users"
    cursor = g.conn.execute(cmd1)
    res = None
    for item in cursor:
        res = item['count1']
    cursor.close()

    cmd = "INSERT INTO users(uid, ctype, gender, birthyear) VALUES (:uid, :ctype, :gender, :birthyear)"
    g.conn.execute(text(cmd), uid = res, ctype = ctype, gender = gender, birthyear = birthyear)
    return redirect('/usersData')

# parent directory for all station data
@app.route('/store')
def store():
    return render_template("store.html")

@app.route('/storeData')
def storeData():

    cursor = g.conn.execute('''
     SELECT store.bid, store.sid, station.name station, arrive_time FROM store, station Where store.bid = station.sid
    ''')
    items = []

    for result in cursor:
        an_item = dict(sid = result['bid'], station = result['station'], bid = result['sid'],
                       arrive_time = result['arrive_time'])
        items.append(an_item)

    cursor.close()

    return render_template("storeData.html", items = items)


def get_arrows(locations, some_map, color= "#E37222", size=6, n_arrows=3):
    '''
    Get a list of correctly placed and rotated
    arrows/markers to be plotted

    Parameters
    locations : list of lists of lat lons that represent the
                start and end of the line.
                eg [[41.1132, -96.1993],[41.3810, -95.8021]]
    arrow_color : default is 'blue'
    size : default is 6
    n_arrows : number of arrows to create.  default is 3
    Return
    list of arrows/markers
    '''

    Point = namedtuple('Point', field_names=['lat', 'lon'])

    # creating point from our Point named tuple
    p1 = Point(locations[0][0], locations[0][1])
    p2 = Point(locations[1][0], locations[1][1])

    # getting the rotation needed for our marker.
    # Subtracting 90 to account for the marker's orientation
    # of due East(get_bearing returns North)
    rotation = get_bearing(p1, p2) - 90

    # get an evenly space list of lats and lons for our arrows
    # note that I'm discarding the first and last for aesthetics
    # as I'm using markers to denote the start and end
    arrow_lats = np.linspace(p1.lat, p2.lat, n_arrows + 2)[1:n_arrows + 1]
    arrow_lons = np.linspace(p1.lon, p2.lon, n_arrows + 2)[1:n_arrows + 1]

    arrows = []

    # creating each "arrow" and appending them to our arrows list
    for points in zip(arrow_lats, arrow_lons):
        arrows.append(folium.RegularPolygonMarker(location=points,
                                                  fill_color=color, number_of_sides=3,
                                                  radius=size, rotation=rotation).add_to(some_map))
    return arrows


def get_bearing(p1, p2):
    '''
    Returns compass bearing from p1 to p2

    Parameters
    p1 : namedtuple with lat lon
    p2 : namedtuple with lat lon

    Return
    compass bearing of type float

    Notes
    Based on https://gist.github.com/jeromer/2005586
    '''

    long_diff = np.radians(p2.lon - p1.lon)

    lat1 = np.radians(p1.lat)
    lat2 = np.radians(p2.lat)

    x = np.sin(long_diff) * np.cos(lat2)
    y = (np.cos(lat1) * np.sin(lat2)
         - (np.sin(lat1) * np.cos(lat2)
            * np.cos(long_diff)))
    bearing = np.degrees(np.arctan2(x, y))

    # adjusting for compass bearing
    if bearing < 0:
        return bearing + 360
    return bearing

@app.route('/login')
def login():
    abort(401)
    this_is_never_executed()

if __name__ == "__main__":
  import click

  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    """
    This function handles command line parameters.
    Run the server using

        python server.py

    Show the help text using

        python server.py --help

    """

    HOST, PORT = host, port
    print "running on %s:%d" % (HOST, PORT)
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)


  run()
