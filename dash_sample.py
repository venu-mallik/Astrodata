import dash,quandl, colorlover as cl,ephem,math,datetime,time,sqlite3
import dash_dangerously_set_inner_html as dds,threading
import dash_core_components as dcc
import dash_html_components as html,dash_table
from datetime import datetime as dt
import dash_bootstrap_components as dbc, gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np,pandas as pd,json,gviz_api,copy,os
from apscheduler.schedulers.background import BackgroundScheduler
colorscale = cl.scales['9']['qual']['Paired']
quandl_apikey = "quandl_key"
sec,read,temp,maindata,df_del,FOLIST,dff =  {},False,'',False,False,[],pd.DataFrame()
file = os.getcwd() + os.path.sep
from snapi_py_client.snapi_bridge import StocknoteAPIPythonBridge
import json,pandas as pd,os,time,websocket
api = StocknoteAPIPythonBridge()
token = None
def login_now():
    global token
    if token is None:
        login = api.login(body={"userId":"userid","password":"password","yob":"1234"})
        login = json.loads(login)
        token = login.get("sessionToken")
        print(token)
        return token
    else :
        return token

if token is None:
    token = login_now()
    api.set_session_token(sessionToken=token)

sched = BackgroundScheduler(deamon=True)

def get_sectors():
    sec = {}
    sec['LOW'], sec["SLOPE_SELL"], sec["SLOPE_BUY"] = [], [], []
    sec['HIGH'], sec["TOP_GAINERS"], sec["TOP_LOSERS"] = [],  [], []
    sec['BBB'] , sec['SSS'],sec['SS'],sec['BB'] = [],[],[],[]
    sec['HOME'] = ['RELIANCE', 'INFY', 'HDFCBANK', 'HDFC', 'ICICIBANK', 'LT', 'HINDUNILVR', 'KOTAKBANK', 'TCS', 'ITC','BAJFINANCE','SBIN']
    sec['BANK'] = ['HDFCBANK', 'ICICIBANK', 'SBIN', 'KOTAKBANK', 'AXISBANK', 'YESBANK', 'INDUSINDBK', 'PNB',
                   'BANKBARODA','FEDERALBNK', 'IDFCFIRSTB', 'RBLBANK']
    sec['NBFC'] = ['HDFC', 'BAJFINANCE', 'BAJAJFINSV',  'CHOLAFIN', 'IDFC', 'MUTHOOTFIN', 'SRTRANSFIN'
                   , 'L&TFH', 'ICICIPRULI', 'MANAPURRAM', 'EQUITAS', 'CANFINHOME',  'SREINFRA', 'IFCI', 'IDBI',
                   'BHARATFIN', 'UJJIVAN', 'REPCOHOME', 'PFC', 'RECLTD']
    sec['PSUBANK'] = ['SOUTHBANK', 'SYNDIBANK', 'IDFCBANK', 'ALBK', 'PNB', 'UNIONBANK', 'BANKINDIA', 'ORIENTBANK',
                      'KTKBANK', 'BANKBARODA', 'DCBBANK', 'INDIANB', 'CANBK', 'SBIN','CENTRALBK','J&KBANK']
    sec['MEDIA_METALS'] = ['SUNTV', 'PVR', 'DISHTV', 'ZEEL', 'INFRATEL', 'BHARTIARTL', 'IDEA','DISHTV'
                           ,'TATASTEEL', 'VEDL', 'NMDC', 'SAIL', 'HINDALCO', 'JINDALSTEL', 'JSWSTEEL', 'NATIONALUM','COALINDIA']
    sec['MISC'] = ['INDIGO', 'JETAIRWAYS', 'PIDILITIND', 'TATACHEM', 'UPL', 'CASTROLIND', 'ASIANPAINT', 'BERGERPAINT',
                   'GSFC', 'MFSL', 'JISLJALEQS', 'KSCL', 'MCX', 'PTC', 'NHPC','ADANIPORTS', 'ADANIPOWER', 'ADANIENT', 'RCOM', 'RELCAPITAL', 'RELINFRA', 'RPOWER',
                    'SUZLON',  'JPASSOCIAT', 'GMRINFRA', 'CGPOWER']
    sec['INFRA'] = ['CESC', 'CONCOR', 'DLF', 'ACC', 'HCC', 'NCC', 'NBCC', 'ULTRACEMCO', 'AMBUJACEM', 'INDIACEM',
                    'RAMCOCEM','DALMIABHA', 'KAJARIACER', 'VOLTAS', 'CENTURYTEX', 'GRASIM', 'LT', 'BEML', 'ENGINERSIN', 'BHEL',
                    'SIEMENS', 'CUMMINSIND', 'BEL']
    sec['IT'] = ['TCS', 'INFY', 'HCLTECH', 'TECHM', 'WIPRO', 'MINDTREE', 'INFIBEAM', 'HEXAWARE', 'KPIT', 'TATAELXSI', 'NIITTECH', 'JUSTDIAL']
    sec['AUTO'] = ['TVSMOTOR', 'MARUTI', 'ASHOKLEY', 'APOLLOTYRE', 'CEATLTD', 'TATAMTRDVR', 'TATAMOTORS', 'EXIDEIND',
                   'BAJAJ-AUTO', 'M&M', 'MOTHERSUMI', 'AMARAJABAT', 'ESCORTS', 'BALKRISIND', 'CASTROLIND', 'BHARATFORG']
    sec['PHARMA'] = ['APOLLOHOSP', 'STAR', 'GLENMARK', 'PEL', 'CADILAHC', 'LUPIN', 'DRREDDY', 'SUNPHARMA', 'BIOCON',
                     'CIPLA', 'AUROPHARMA', 'DIVISLAB', 'AJANTPHARM',  'STAR', 'TORNTPHARM', 'WOCKPHARMA']
    sec['METALS'] = ['TATASTEEL', 'VEDL', 'NMDC', 'SAIL', 'HINDALCO', 'JINDALSTEL', 'JSWSTEEL', 'NATIONALUM','HINDZINC','COALINDIA']
    sec['FMCG'] = [ 'TITAN', 'COLPAL', 'DABUR', 'BRITANNIA', 'NESTLEIND', 'BATAINDIA', 'JUBLFOOD',
                     'ITC', 'MARICO', 'UBL', 'GODREJCP', 'HINDUNILVR', 'TATAGLOBAL',
                   'MCDOWELL-N',  'VOLTAS',  'CENTURYTEX', 'HAVELLS',  'SRF', 'PAGEIND']
    sec['ENERGY'] = ['INDIGO',  'TATAPOWER', 'TORNTPOWER', 'ADANIPOWER', 'PETRONET', 'OIL', 'ONGC', 'NTPC',
                     'GAIL', 'POWERGRID', 'IOC', 'RELIANCE', 'BPCL', 'HINDPETRO', 'IGL', 'MGL', 'CHENNPETRO']
    sec['GUJJU'] = ['ADANIPORTS', 'ADANIPOWER', 'ADANIENT', 'RCOM', 'RELCAPITAL', 'RELINFRA', 'RPOWER',
                    'SUZLON',  'JPASSOCIAT', 'GMRINFRA', 'CGPOWER']
    return sec

sec = get_sectors()


p = ['mars', 'jupiter', 'saturn', 'sun', 'moon', 'rahu', 'mercury', 'venus','ketu' ,'uranus','neptune','pluto']
types = ['Default','D1_Transit', 'Retrograde', 'Conjunction', 'D9_Transit','Nakshtra_Transit', 'Vargottama', 'Ucha & Neecha'  ]
type_of_plot = [ {'label':i.title(),'value':i} for i in types ]
sectors_to_plot =  [ {'label':i.title(),'value':i.upper()} for i in sec.keys() ]
pl_to_plot =  [{'label':i.title(),'value':i.lower()} for i in p ]
timeframe = [ {'label':i.title(),'value':i.lower()} for i in [ '1min','2min','3min','4min','5min','10min','15min','30min','45min','60min','75min','90min','120min','180min','240min'        ]                ]
tempticker = None
VALID_USERNAME_PASSWORD_PAIRS = { 'pass':'pass'}
MenuHtml = """ 	    <ul class="nav nav-tabs">
  <li class="active"><a href="/chart/home">Home</a></li>
  <li ><a href="/chart/grid">Grid</a></li>
  <li ><a href="/chart/astro">Astro</a></li>   </ul>
 """

"""<li class="dropdown">
    <a class="dropdown-toggle" data-toggle="dropdown" >Custom</a>    <ul class="dropdown-menu">
      <li><a href="/chart/LOW">DayLow</a></li>       <li><a href="/chart/HIGH">DayHigh</a></li>
      <li><a href="/chart/TOP_GAINERS">TOP_GAINERS</a></li>       <li><a href="/chart/TOP_LOSERS">TOP_LOSERS</a></li>
      <li><a href="/chart/SLOPE_SELL">SLOPE_SELL</a></li>       <li><a href="/chart/SLOPE_BUY">SLOPE_BUY</a></li>
    </ul>   </li>"""


def lahiriayanamsa(dff):
    ayy = []
    for i in dff.index:
        A = (16.90709 * i.year / 1000) - 0.757371 * (i.year / 1000) * (i.year / 1000) - 6.92416100010001000
        B = ((i.month - 1 + i.day) / 30) * 1.1574074 / 1000
        ayy.append(A + B)
    dff['lahiri'] = ayy
    return dff

def get_thithi(dff):
    dg, arr = 2.0 * ephem.pi, []
    for i in dff.index:
        dg = dff.loc[i]['moon'] - dff.loc[i]['sun']
        if dg < 0:
            dg = dg + 360
        dg = dg // 12
        arr.append(dg + 1)
    dff['thithi'] = arr
    return dff

def get_navamsha(dff):
    p = ['mars', 'jupiter', 'saturn', 'sun', 'moon', 'rahu', 'mercury', 'venus', 'ketu', 'neptune', 'uranus',
         'pluto']
    for i in p:
        dff[i + '_nm'] = 30 * (((dff[i] * 60) % 2400) / 200)
    for i in p:
        dff[i + "_n_h"] = (1 + ((dff[i] * 60) % 2400) // 200)
    for i in p:
        dff[i + "_h"] = 1 + (dff[i] // 30)
    for i in p:
        dff[i + "_nak"] = (1 + ((dff[i] * 60) % 2400) // 800)
    return dff

def node(dff):
    date = datetime.datetime(1900, 1, 1)
    rahu = []
    ketu = []
    for i in dff.index:
        now = i
        delta = now - date
        d = delta.days
        # print(d)
        t = 1.018
        deg = 259.183 - 0.05295 * (d + 1) + 0.002078 * math.pow(t, 2) + 0.000002 * math.pow(t, 3)
        val = deg % 360 - dff.loc[i]['lahiri']
        if val < 0:
            val = 360 - abs(val)
        rahu.append(val)
        if val > 180:
            ketu.append((val + 180) % 360)
        else:
            ketu.append(val + 180)
    dff['rahu'] = rahu
    dff['ketu'] = ketu
    return dff

def phases_moon(dff):
    tau, arr = 2.0 * ephem.pi, []
    sun = ephem.Sun()
    moon = ephem.Moon()
    names = ['Waxing Crescent', 'Waxing Gibbous',
             'Waning Gibbous', 'Waning Crescent']
    for i in dff.index:
        t = datetime.time(hour=13, minute=30)
        s = datetime.datetime.combine(i, t)
        sun.compute(s)
        moon.compute(s)
        sunlon = ephem.Ecliptic(sun).lon
        moonlon = ephem.Ecliptic(moon).lon
        angle = (moonlon - sunlon) % tau
        quarter = int(angle * 4.0 // tau)
        arr.append(quarter * 90)
    dff['moonphase'] = arr
    return dff


def degrees(df, pla):
    if pla == 'moonphase':
        return phases_moon(df)
    e = ephem.Observer()
    e.lon, e.lat, e.elevation = '72:52:57.2', '19:04:22.4', 14
    planets = {'moon': ephem.Moon(), 'sun': ephem.Sun(), 'mars': ephem.Mars(), 'mercury': ephem.Mercury(),
               'jupiter': ephem.Jupiter(), 'saturn': ephem.Saturn(), 'venus': ephem.Venus(),
               'neptune': ephem.Neptune(), 'uranus': ephem.Uranus(), 'pluto': ephem.Pluto()}
    for pl, obj in planets.items():
        if pl == pla:
            moon, arr = obj, []
            for i in df.index:
                t = datetime.time(hour=13, minute=30)
                s = datetime.datetime.combine(i, t)
                e.date, e.epoch, dg = s, s, 0
                moon.compute(e)
                dg = (moon.g_ra / ephem.degree) - df.loc[i]['lahiri']
                if dg < 0:
                    dg = 360 - abs(dg)
                arr.append(dg)
            df[pla] = arr
            return df
    if pla == 'yoga':
        df[pla] = (((df.moon + df.sun) // 13.20) % 27) * 10
    return df

def retromarks(dff):
    plan = ['mars', 'jupiter', 'saturn', 'mercury', 'venus', 'uranus', 'neptune', 'pluto']
    for i in plan:
        dff[i + "_r"] = (dff[i] > dff[i].shift(-2))
    return dff

def speed_of_planet(dff):
    plan = ['mars', 'jupiter', 'saturn', 'mercury', 'venus']
    planets = {'moon': ephem.Moon(), 'sun': ephem.Sun(), 'mars': ephem.Mars, 'mercury': ephem.Mercury,
               'jupiter': ephem.Jupiter, 'saturn': ephem.Saturn, 'venus': ephem.Venus, 'neptune': ephem.Neptune(),
               'uranus': ephem.Uranus(), 'pluto': ephem.Pluto()}

    def hpos(body):
        return body.hlon, body.hlat

    for i in plan:
        body = planets[i]
        dic = []
        for j in dff.index:
            dic.append(ephem.separation(hpos(body(j)), hpos(body(j + datetime.timedelta(days=1)))))
        # dff[i+"_speed"] = (dff[i] - dff[i].shift(-1))
        dff[i + "_speed"] = dic
        dff[i + "_speed"] = dff[i + "_speed"].abs() * 1000
    return dff

def get_weekday(dff):
    dff['weekday'] = dff.index.dayofweek
    return dff

def prepare_astro_data():
    p = ['mars', 'jupiter', 'saturn', 'sun', 'moon', 'rahu', 'mercury', 'venus', 'ketu', 'uranus', 'neptune',
         'pluto']
    ext = {'mars': [297, 298, 299], 'jupiter': [94, 95, 96], 'saturn': [139, 140, 141], 'sun': [9, 10, 11],
           'moon': [31, 32, 33, 34, 35, 36], 'mercury': [165, 166, 167], 'venus': [356, 357, 358]}
    deb = {'mars': [117, 118, 119], 'jupiter': [274, 275, 276], 'saturn': [19, 20, 21], 'sun': [189, 190, 191],
           'moon': [211, 212, 213, 214, 215, 216], 'mercury': [345, 346, 347], 'venus': [176, 177, 178]}
    astrodf = pd.read_csv(file+"astro_data.csv")
    astrodf.index = [pd.to_datetime(ma, format="%Y-%m-%d") for ma in astrodf.Date]
    astrodf = get_thithi(astrodf)
    astrodf = retromarks(astrodf)
    astrodf = get_navamsha(astrodf)
    astrodf['daycount'] =  [int(i.days) for i in (  astrodf.index.max() - astrodf.index)  ]
    for i in p:
        astrodf[i + '_d1_t'] = astrodf[i + '_h'].diff()
        astrodf[i + '_d9_t'] = astrodf[i + '_n_h'].diff()
        astrodf[i + '_nak_t'] = astrodf[i + '_nak'].diff()
    for i, j in ext.items():
        astrodf[i + "_et"] = astrodf[i].astype(int)
        astrodf[i + '_nm' + "_et"] = astrodf[i + "_nm"].astype(int)
        astrodf[i + "_et"] = astrodf[i + "_et"].isin(j)
        astrodf[i + '_nm' + "_et"] = astrodf[i + '_nm' + "_et"].isin(j)
    for i, j in deb.items():
        astrodf[i + "_dt"] = astrodf[i].astype(int)
        astrodf[i + '_nm' + "_dt"] = astrodf[i + "_nm"].astype(int)
        astrodf[i + "_dt"] = astrodf[i + "_dt"].isin(j)
        astrodf[i + '_nm' + "_dt"] = astrodf[i + '_nm' + "_dt"].isin(j)
    return astrodf

astrodff = prepare_astro_data()

def get_planet_data_strings(planets, mindate, maxdate, close):
    global astrodff
    astrodf = astrodff[mindate:maxdate].copy(deep=True)
    print(len(astrodf), len(astrodff))
    li = []
    for i in planets:
        astrodf[i] = astrodf[i].astype(int)
        if i == "daycount":
            astrodf[i] = [int(dc) for dc in ( astrodf[i] - astrodf[i].min() )  ]
        tempdf = astrodf[(astrodf[i.lower()] != 0) | (astrodf[i.lower()] != False)]
        li.append(
            {'x': tempdf.index, 'y': tempdf[i.lower()], 'xaxis': 'x', 'yaxis': 'y2', 'name': i, 'type': 'scatter',
             'mode': 'markers'})
    return li

def ma_strat(i,fun ,stdate,edate):
    import pandas as pd, numpy as np, os, Indicators, talib as ta
    folder = "C:\\Users\\Venu Mallik\\Desktop\\data\\Google_drive\\Futures\\"
    fields = ['ticker', 'odate', 'otime', 'open', 'high', 'low', 'close', 'volume', 'openint', 'b1', 'b2']
    df = pd.read_csv(folder + "NIFTY_F1.txt",  names=fields)
    df['datetime'] = df.odate.astype('str') + " " + df.otime.astype('str')
    df.datetime = pd.to_datetime(df['datetime'] , format="%Y%m%d %H:%M")
    df.set_index('datetime', inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    df.sort_index(inplace=True, ascending=True)
    dfrepot = pd.DataFrame()
    funct = ta.get_function_groups()
    inde = np.arange(1,181)
    data = { 'timeframe' : inde}
    #for fun in funct['Pattern Recognition']:
    result = getattr(ta,fun)
    data[fun] = []
    dfhr = df.resample(str(i)+'min',).apply(  {   'open':'first' , 'high':'max' , 'low':'min' , 'close':'last' , 'volume':'sum' , 'openint':'sum'  })
    dfhr = dfhr.dropna()
    #print(dfhr.to_string())
    dfhr[fun] = result(dfhr.open , dfhr.high , dfhr.low , dfhr.close)
    co = len(dfhr[dfhr[fun] != 0])


def get_layout():
    with open(file+'stockslist_bse_quandl.txt') as f:
        arr = f.readlines()
    with open(file+'indiceslist_bse_quandl.txt') as f:
        indexes = f.readlines()
    sym = [{'label': i.split("|")[0].strip(), 'value': "BSE/" + i.split("|")[1].strip()} for i in arr if
           i.strip() != ""]
    sym = sym + [{'label': "dow", 'value': "BCB/UDJIAD1"},{'label': "dow part 1", 'value': "FRED/M1109AUSM293NNBR"},
                 {'label': "dow part 2", 'value': "FRED/M1109BUSM293NNBR"},  {'label': 'sensex', 'value': "BSE/SENSEX"}]
    sym = sym + [{'label': i.split("|")[0].strip(), 'value': "BSE/" + i.split("|")[1].strip()} for i in indexes if
                 i.strip() != ""] + [{'label': 'NIFTY_F1', 'value': 'NIFTY_F1'}]
    baselayout = html.Div( [ html.Div([  dds.DangerouslySetInnerHTML(MenuHtml)
                               ]),
                          dcc.Location(id='url', refresh=True),
        html.Div(id="home",className="row",style={"height":"auto"}),
        dcc.Textarea(        id='textarea',        value='Textarea content initialized\nwith multiple lines of text',
        style={'width': 'auto', 'height': 20},    )
                               ], style={"margin-left":"10px"})
    astrolayout = html.Div( [
            dbc.Row([dbc.Col(dcc.Dropdown(id='turn_type', options=type_of_plot,
                  style={"width": "250px", "height": "30px", "overflow": "visible"}, value="Default",
                   placeholder="Select ..."), width="auto"),
                  dbc.Col(dcc.Dropdown(id='planet_to_plot', style={"width": "500px", "height": "30px","overflow": "visible"},
                    options=pl_to_plot,value=[], multi=True), width="auto"),
                    ],   no_gutters=True),
            dbc.Row([ dbc.Col(dcc.Dropdown(id='my-dropdown', options=sym,
                    style={"width": "250px", "height": "50px","overflow": "visible"}, value='BSE/SIBANK'),width='auto'),
                  dbc.Col(dcc.DatePickerRange(id='datepicker', min_date_allowed=dt(1900, 1, 1),
                    max_date_allowed=dt.today() + datetime.timedelta(days=500)), width="auto")], no_gutters=True),
                        dcc.Graph(id='my-graph', style={"width":"1200px"} ) ],className="" , style={"margin-left":"10px"} )
    return [baselayout,astrolayout]  # end_date=dt(2019,6,6)

def otherLayout():
    return html.Div( [  dbc.Col(dcc.Dropdown(id='urlsec',  style={"width": "500px", "height": "30px", "overflow": "visible"},
                            options=sectors_to_plot, value="HOME"), width="auto"),
                               html.Div(id='graphs', className="row" , style={"height":"auto"}),
                                ], style={"margin-left":"10px"} )


def gridLayout():
    pass


ext_css = ["https://fonts.googleapis.com/css?family=Product+Sans:400,400i,700,700i",
 "https://cdn.rawgit.com/plotly/dash-app-stylesheets/2cc54b8c03f4126569a3440aae611bbef1d7a5dd/stylesheet.css",  "https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css",
"https://stackpath.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css",  "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css",
{'rel' : 'stylesheet','href' : "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css",'integrity' :"sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u",'crossorigin': "anonymous" }]
ext_scripts = [ { 'src':"https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"},
{'src' : "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js",'integrity': "sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa",
'crossorigin': 'anonymous'},{'src':"https://kite.trade/publisher.js?v=3"}]

meta_ts = [{'name': 'AstroFinance Visualisation',
            'content': 'Nifty Astrology Navamsha vargottama retrograde gann moon phases '},
           {'http-equiv': 'X-UA-Compatible', 'content': 'IE=edge'},
           {'name': 'viewport', 'content': 'width=device-width, initial-scale=1.0'},]
astroapp = dash.Dash('Astro Nifty', external_stylesheets=ext_css,external_scripts=ext_scripts, meta_tags=meta_ts,suppress_callback_exceptions=True)
astroapp.scripts.config.serve_locally = False
astroapp.title = "VRworkers"
astroapp.layout = get_layout()[0]



@astroapp.callback(dash.dependencies.Output('home', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def get_app(ur):
    lay = get_layout()
    if ur == "/chart/astro":
        return lay[1]
    elif ur == "/chart/grid":
        df = pd.read_csv(file+"liveboard.csv",header=0)
        layout = dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in ["Mcap",'open','low','high','F.Low','F.High','OpenLB:HS','Signal','V/30V','STOCK','CMP','%CMP','5K.Qty']],
                data=df.to_dict('records'),
                filter_action="native",
                sort_action="native",
                sort_mode="multi"
            )
        return layout
    else :
        return otherLayout()


@astroapp.callback(dash.dependencies.Output('my-graph', 'figure'),
                   [dash.dependencies.Input('planet_to_plot', 'value'),
                    dash.dependencies.Input('my-dropdown', 'value')],
                   [dash.dependencies.State('datepicker', 'start_date'),
                    dash.dependencies.State('datepicker', 'end_date')])
def get_update_graph(planets, ticker=None, stdate=None, edate=None):
    global tempticker
    if stdate is not None and edate is not None:
        dff = quandl.get(ticker, api_key=quandl_apikey, start_date=stdate, end_date=edate)
        tempticker = ticker
    if stdate is None and edate is None:
        dff = quandl.get(ticker, api_key=quandl_apikey, start_date=dt.today() + datetime.timedelta(days=-500),
                         end_date=dt.today())
        tempticker = ticker
    dff.index = [pd.to_datetime(ma, format="%Y-%m-%d") for ma in dff.index]
    if 'Open' not in dff.columns:
        dff['Close'] = dff.Value
        dff['Open'] = dff.Value
        dff['High'] = dff.Value
        dff['Low']  = dff.Value
    dff.Open = dff.Open.fillna(value=dff.Close - 1)
    dff.High = dff.High.fillna(value=dff.Close + 2)
    dff.Low = dff.Low.fillna(value=dff.Close - 2)
    # dff = dff.resample('D').fillna(method='ffill')
    # print(dff.tail(10).to_string())
    li = []
    candlestick = {'xaxis': 'x', 'x': dff.index, 'yaxis': 'y1', 'open': dff['Open'], 'high': dff['High'],
                   'low': dff['Low'], 'close': dff['Close'],
                   'type': 'candlestick', 'name': ticker, 'increasing': {'line': {'color': 'green'}},
                   'decreasing': {'line': {'color': 'red'}}}
    if len(planets) > 0 and stdate is None:
        li = get_planet_data_strings(planets, dff.index.min(), dff.index.max(), dff.Close.max())
    if len(planets) > 0 and stdate is not None:
        li = get_planet_data_strings(planets, stdate, edate, dff.Close.max())
        # moon = { 'x': dff.index,'y': dff['moon'],  'xaxis':'x' , 'yaxis':'y2','name':'moon','type':'scatter','mode':'lines' }
        # sun = {'x': dff.index, 'y': dff['sun'], 'xaxis': 'x', 'yaxis': 'y2','name':'sun', 'type': 'scatter', 'mode': 'lines'}
    return {'data': [candlestick] + li,
            'layout': {"spikedistance": 200, "hoverdistance": 25, 'margin': {'l': 40, 'r': 0, 't': 20, 'b': 30},
                       'xaxis': {"spikemode": "across", 'showspikes': True, "spikedash": "solid",
                                 'rangeslider': {'visible': False}},
                       'yaxis1': {'domain': [0.45, 1]},
                       'yaxis2': {'showgrid': True, 'dtick': 30, 'domain': [0.1, 0.45]}}}

@astroapp.callback(dash.dependencies.Output('planet_to_plot', 'options'),
                   [dash.dependencies.Input('turn_type', 'value')])
def populate_dropdown(x):
    p = ['mars', 'jupiter', 'saturn', 'sun', 'moon', 'rahu', 'mercury', 'venus', 'ketu', 'uranus', 'neptune',
         'pluto']
    t = ['Default', 'D1_Transit', 'Retrograde', 'Conjunction', 'D9_Transit', 'Nakshtra_Transit', 'Vargottama',
         'Ucha & Neecha']
    if x.lower() == t[0].lower():
        dummy = p + ['thithi','daycount']
        return [{'label': i.title(), 'value': i.lower()} for i in dummy]
    if x.lower() == t[1].lower():
        return [{'label': i.title(), 'value': i + '_d1_t'} for i in p]
    if x.lower() == t[4].lower():
        return [{'label': i.title(), 'value': i + '_d9_t'} for i in p]
    if x.lower() == t[5].lower():
        return [{'label': i.title(), 'value': i + '_nak_t'} for i in p]
    if x.lower() == t[2].lower():
        return [{'label': i.title(), 'value': i + '_r'} for i in
                ['mars', 'jupiter', 'saturn', 'mercury', 'venus', 'uranus', 'neptune', 'pluto']]
    if x.lower() == t[3].lower():
        return [{'label': i.title(), 'value': i} for i in p] +[
            {'label': i + '_d9'.title(), 'value': i + '_nm'} for i in p]
    if x.lower() == t[6].lower():
        return [{'label': i.title(), 'value': i + "_h"} for i in p] + [
            {'label': i + '_d9'.title(), 'value': i + "_n_h"} for i in p]
    if x.lower() == t[7].lower():
        ar = ['mars', 'jupiter', 'saturn', 'sun', 'moon', 'mercury', 'venus']
        rl = [{'label': i + '_et'.title(), 'value': i + '_et'} for i in ar] + [
            {'label': i + '_dt'.title(), 'value': i + '_dt'} for i in ar]
        nl = [{'label': i + '_nm' + '_et'.title(), 'value': i + '_et'} for i in ar] + [
            {'label': i + '_nm' + '_dt'.title(), 'value': i + '_dt'} for i in ar]
        return rl + nl


def get_stock_data(ticker):
    dff = quandl.get(ticker, api_key=quandl_apikey)
    stdate = None
    edate = None
    dff.index = [pd.to_datetime(ma, format="%Y-%m-%d") for ma in dff.index]
    dff.Open = dff.Open.fillna(value=dff.Close - 1)
    dff.High = dff.High.fillna(value=dff.Close + 2)
    dff.Low = dff.Low.fillna(value=dff.Close - 2)
    return dff

def get_stock_chart(type, dff, ticker):
    if type == 'CANDLESTICK':
        return {'xaxis': 'x', 'x': dff.index, 'yaxis': 'y1', 'open': dff['Open'], 'high': dff['High'],
                'low': dff['Low'], 'close': dff['Close'],
                'type': 'candlestick', 'name': ticker, 'increasing': {'line': {'color': 'green'}},
                'decreasing': {'line': {'color': 'red'}}}
    else:
        return {'x': dff.index, 'y': dff['Close'], 'xaxis': 'x', 'yaxis': 'y1', 'name': ticker, 'type': 'scatter',
                'mode': 'lines'}

@astroapp.callback(dash.dependencies.Output('graphs', 'children'),
              [dash.dependencies.Input('urlsec', 'value')])
def jsondata(sector):
    global  maindata, sec, temp
    graph, tempChart = [], None
    allSYM = [column for column in maindata['STOCK'] if column != "time"]
    symList = [str(i) for i in sec[sector]]
    if sector == "astro":
        return graph
    if sector == None or sector == 'HOME':
        symList = sec['HOME']
    for i in symList:
        try:
            ticker = i.replace('&', 'and')
            ar = maindata[maindata["STOCK"]==i]
            ar = ar.drop_duplicates('time', keep='last')
            ar["CMP"] = ar["CMP"].astype(float)
            r = np.mean(ar['CMP'])
            maxa = np.max(ar['CMP'])
            mina = np.min(ar['CMP'])
            siz=5000//mina
            title = i + " L:" + str(mina) + " H:" + str(maxa) + "," +str(  round(list(ar['CMP'])[-1], 1))
            titl= i + "," +str(  round(list(ar['CMP'])[-1], 1))+","+str(maxa)+","+str(mina)
            '''tempChart = {'x': ar['time'], 'y': ar['CMP'] , 'xaxis': 'x', 'yaxis': 'y', 'name': ticker, 'type': 'scatter',
             'mode': 'lines'}'''
            tempChart = {'xaxis': 'x', 'x': ar['time'], 'yaxis': 'y1', 'open': ar['open'], 'high': ar['high'],
                'low': ar['low'], 'close': ar['close'],
                'type': 'candlestick', 'name': ticker, 'increasing': {'line': {'color': 'green'}},
                'decreasing': {'line': {'color': 'red'}}}
            btnHtml = str(titl)+ ''' <a href="#" id="'''+str(ticker)+"_buy"+'''" data-kite="f0oj12fff7za8fks" data-exchange="NSE" data-product="MIS" class="btn btn-default btn-sm"
                    data-tradingsymbol="'''+str(ticker)+'''" data-transaction_type="BUY" data-quantity='''+str(int(siz)) +'''    data-order_type="MARKET" >B</a>
                    <a href="#" id="'''+str(ticker)+"_sell"+'''" data-kite="f0oj12fff7za8fks" data-exchange="NSE" data-product="MIS" class="btn btn-default btn-sm"
                    data-tradingsymbol="'''+str(ticker)+'''" data-transaction_type="SELL" data-quantity='''+str(int(siz)) +'''
                     data-order_type="MARKET">S</a>
                     <script type="text/javascript"> KiteConnect.ready(function() {    var kite = new KiteConnect("f0oj12fff7za8fks");
                      kite.link("#'''+str(ticker)+"_buy"+'''"); kite.link("#'''+str(ticker)+"_sell"+'''"); }); </script> '''
            graph.append(  html.Div( [dds.DangerouslySetInnerHTML(btnHtml)  ,dcc.Graph(
                id=ticker, style={"width": "325px", "height": "175px"},
                figure={ 'title':title,     'data': [tempChart],  # + bollinger_traces,
                    'layout': {'xaxis': { 'rangeslider': {'visible': False}},
                               'margin': {'b': 15, 'r': 0, 'l': 28, 't': 0}, 'legend': {'x': 0}
                               } }, config={'displayModeBar': False} )],style={"width": "auto", "height": "auto"},  className="col-md-3"  )  )
        except Exception as e:
            print("traceback  ---> ",flush=True)
            pass
    temp = sector
    symList = [str(i) for i in sec[sector]]
    return graph



def getChartData():
    global maindata, sec, symList, file, read
    maindata = None
    slope, intercept, arr = 0, 0, []
    maindata = pd.read_csv(file+"intradf.csv", index_col=0, header=0,parse_dates=True, infer_datetime_format=True)#.T
    if len(maindata.index) == 0:
        maindata = pd.read_csv(file + "intradf.csv", index_col=0, skiprows=0, parse_dates=True,infer_datetime_format=True)
    df = maindata
    df["CMP"] = df["CMP"].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    df["%CMP"] = df["%CMP"].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    for column in df["STOCK"].unique():
        c = column
        ar = df[df["STOCK"]==c]
        if len(ar['CMP']) > 5 :
            try:
                data = {'values': ar['CMP'][-5:], 'Time': [0, 1, 2, 3, 4]}
                sdf = pd.DataFrame(data, columns=['values', 'Time'])
                sdf.index = sdf['Time'].astype(int)
                del sdf['Time']
                slope, intercept = np.polyfit(range(len(sdf.index)), sdf['values'], 1, full=False)
            except Exception:
                slope, intercept = 0 , 0
                pass
        try:
            arr.append({'SYM': c, 'SLOPE': float(slope) })
        except:
            continue
    df = pd.DataFrame(arr)
    sec["SLOPE_SELL"] = [i for i in df.sort_values('SLOPE').head(30)['SYM']]
    sec["SLOPE_BUY"] = [i for i in df.sort_values('SLOPE').tail(30)['SYM']][::-1]
    del df
    df = pd.read_csv(file+"liveboard.csv",header=0)
    sec["Day_LOW"] = [i for i in df.sort_values('F.Low').head(30)['STOCK']]
    sec["Day_HIGH"] = [i for i in df.sort_values('F.High').head(30)['STOCK']]
    sec["TOP_GAINERS"] = [i for i in df.sort_values('%CMP').tail(30)['STOCK']][::-1]
    sec["TOP_LOSERS"] = [i for i in df.sort_values('%CMP').head(30)['STOCK']]
    sec["BBB"] = [i for i in df[df['Signal']=="BBB"]['STOCK'] ][:30]
    sec["SSS"] = [i for i in df[df['Signal']=="SSS"]['STOCK'] ][:30]
    sec["BB"] = [i for i in df[df['Signal']=="BB"]['STOCK'] ][:30]
    sec["SS"] = [i for i in df[df['Signal']=="SS"]['STOCK'] ][:30]
    del df


def getDynamicSheetPrice():
    tdff = pd.DataFrame()
    offset = datetime.timezone(datetime.timedelta(hours=5,minutes=30))
    date = datetime.datetime.now()
    global file
    morn = date.replace(hour=9,minute=00,second=00)
    eve = date.replace(hour=16,minute=00)
    filenow = "INTRA_"+date.date().strftime("%d%m%Y")+".csv"
    if morn <= date and date < eve and date.date().weekday() not in [5,6]:
        date= datetime.datetime.now()
        try:
            scope = ['https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name('auth_serviceAC_gdrive.json', scope)
            client = gspread.authorize(creds)
            sheet = client.open("NSE LIVE").sheet1
            dfr = sheet.get("A1:P400")
            dfr = pd.DataFrame(dfr[1:],columns=dfr[0])
            dfr.to_csv(file+"liveboard.csv")
            dfr = sheet.get("M1:P400")
            dfr = pd.DataFrame(dfr[1:],columns=dfr[0])
            dfr["time"] = date
            '''if os.path.exists(file+filenow):
                tdff = pd.read_csv(file+filenow, index_col=0, header=0,parse_dates=True, infer_datetime_format=True)#.T
                tdff = tdff.append(dfr)
                tdff.to_csv(file+filenow)
                tdff.to_csv(file+"intradf.csv")
            else:
                dfr.to_csv(file+filenow)
                dfr.to_csv(file+"intradf.csv")
            '''
        except Exception:
            print('exception fetching'+ str(time.localtime()) , flush=True)
            pass
    getChartData()


def getDynamicSamcoPrice():
    tdff = pd.DataFrame()
    offset = datetime.timezone(datetime.timedelta(hours=5,minutes=30))
    date = datetime.datetime.now()
    global file
    morn = date.replace(hour=9,minute=00,second=00)
    eve = date.replace(hour=16,minute=00,second=00)
    filenow = "INTRA_"+date.date().strftime("%d%m%Y")+".csv"
    if morn <= date and date < eve and date.date().weekday() not in [5,6]:
        date= datetime.datetime.now()
        pm = date + datetime.timedelta(minutes=-3)
        pm = pm.strftime("%Y-%m-%d %H:%M:%S")
        cm = date.strftime("%Y-%m-%d %H:%M:%S")
        df = pd.read_csv(file + "src_names.csv", header=0)
        df = df.head(50)
        for sym in df["STOCK"]:
            try:
                ticker = api.get_intraday_candle_data(sym, exchange=api.EXCHANGE_NSE, from_date=pm,to_date=cm)
                time.sleep(0.1)
                ticker = json.loads(ticker)
                li = ticker.get("intradayCandleData")
                print(ticker.get("serverTime"))
                if li:
                    t = li[0].get("dateTime")[:19]
                    dfr = pd.DataFrame(li)
                    dfr["time"] = t
                    dfr["CMP"] = li[0].get("close")
                    dfr["STOCK"]= sym
                    dfr["%CMP"] = 1
                    if os.path.exists(file+filenow):
                        tdff = pd.read_csv(file+filenow, index_col=0, header=0,parse_dates=True, infer_datetime_format=True)#.T
                        tdff = tdff.append(dfr)
                        tdff.to_csv(file+filenow)
                        tdff.to_csv(file+"intradf.csv")
                    else:
                        dfr.to_csv(file+filenow)
                        dfr.to_csv(file+"intradf.csv")
            except Exception as e:
                import traceback
                print('exception fetching '+ str(time.localtime()) + "   " + str(e), flush=True)
                traceback.print_exc()
                pass
        getChartData()


def stream_inputs():
    cols = ["sym", "avgpr", "o", "h", "l", "c", "vol", "oi", "oic", "ltt", "ltp", "chper", "tbq", "tsq"]
    df = pd.read_csv(file + "src_names.csv", header=0)
    df = df.head(400)
    dfm = pd.read_csv("https://developers.stocknote.com/doc/ScripMaster.csv",header=0)
    dfm = dfm[dfm['exchange'] == 'NSE' ]
    dfm = dfm[['name','symbolCode']]
    symbol = list(df['STOCK'])
    listream = []
    for ind,row in dfm.iterrows():
        for sym in symbol:
            if sym == row["name"]:
                listream.append({"symbol": row['symbolCode']})
    print(listream)

    def on_message(ws, msg):
        ms = json.loads(msg)
        res = ms.get("response")
        quote = res.get("data")
        sym = quote.get("sym")
        ltp = quote.get("ltp")
        chper = quote.get("chPer")
        o = quote.get("o")
        h = quote.get("h")
        l = quote.get("l")
        c = quote.get("c")
        ltt = quote.get("ltt")
        if ltt:
            ltt = ltt.replace(",", "")
        tbq = quote.get("tBQ")
        tsq = quote.get("tSQ")
        oic = quote.get("oIChg")
        oi = quote.get("oI")
        vol = quote.get("vol")
        avgPr = quote.get("avgPr")
        csvline = sym + "," + ','.join([avgPr, o, h, l, c, vol, oi, oic, ltt, ltp, chper, tbq, tsq]) + "\n"
        print(sym , ltt)
        with open(file+"data/"+sym+".txt", "a+") as f:
            f.write(csvline)

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("Connection Closed")
        job=sched.get_job("stream")
        job.func()

    def on_open(ws):
        li = json.dumps(listream)
        data = '{"request":{"streaming_type":"quote", "data":{"symbols":'+li +'}, "request_type":"subscribe", "response_format":"json"}}'
        ws.send(data)
        ws.send("\n")

    headers = {'x-session-token': token}
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://stream.stocknote.com", on_open=on_open, on_message=on_message, on_error=on_error,
                                on_close=on_close, header=headers,keep_running=True)
    wst=threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()


sched.add_job(getDynamicSamcoPrice,trigger='interval',minutes=1)
sched.start()
getDynamicSheetPrice()

if __name__ == '__main__':
    astroapp.run_server(host='0.0.0.0',port=8988,debug=False)
