'''The following code is helpful in computing approximate sidereal degrees using Pyephem . 
All the licenses applicable for Pyepehem are also applicable here '''

import pandas as pd,matplotlib.pyplot as plt , numpy as np,quandl
import ephem,time , datetime
from ephem import cities
import math
def thithi(dff):
    dg,arr = 2.0 * ephem.pi,[]
    for i in dff.index:
        dg = dff.loc[i]['moon'] - dff.loc[i]['sun']
        if dg < 0:
            dg = dg+360
        dg = dg /12
        arr.append(int(dg)+1)
    dff['thithi'] = arr
    return dff

def get_weekday(dff):
    dff['weekday'] = dff.index.dayofweek
    return dff

def get_navamsha(dff):
    p = ['mars', 'jupiter', 'saturn', 'sun', 'moon', 'rahu', 'mercury', 'venus']
    for i in p:
        dff[i + '_nm'] = 30*(((dff[i]*60) % 2400)/200)
        dff[i + "_n_h"] =  (1 + ((dff[i]*60) % 2400)//200)
        dff[i+"_h"] = 1 + (dff[i]//30)
    return dff

def lahiriayanamsa(dff):
    ayy = []
    for i in dff.index:
        A = (16.90709*i.year/1000) - 0.757371 *(i.year/1000)*(i.year/1000) - 6.92416100010001000
        B = ((i.month-1+i.day)/30)  *  1.1574074/1000
        ayy.append(A+B)
    dff['lahiri'] = ayy
    return dff

def node(dff ):
    date = datetime.datetime(1900,1,1)
    rahu = []
    ketu = []
    for i in dff.index:
        now = i
        delta = now-date
        d = delta.days
        #print(d)
        t=1.018
        deg  = 259.183 - 0.05295*(d+1) + 0.002078* math.pow(t,2) + 0.000002*math.pow(t,3)
        val = deg % 360 - dff.loc[i]['lahiri']
        if val < 0:
            val = 360 - abs(val)
        rahu.append(val)
        if val > 180 :
            ketu.append((val+180)%360)
        else:
            ketu.append(val+180)
    dff['rahu']=rahu
    dff['ketu'] = ketu
    return dff


def degrees(df ,pla):
    e = ephem.Observer()
    e.lon ,e.lat,e.elevation= '72:51:22.2','19:01:03.4',12.40882
    years = []
    if pla =="rahu" or pla =="ketu" :
        df = node(df)
    planets = {'moon':ephem.Moon(),'sun':ephem.Sun(),'mars':ephem.Mars(),'mercury':ephem.Mercury(),
               'jupiter':ephem.Jupiter(),'saturn':ephem.Saturn(),'venus':ephem.Venus() , 'neptune' : ephem.Neptune(),'uranus':  ephem.Uranus() }
    for pl ,obj in planets.items():
        if pl == pla:
            moon ,arr= obj,[]
            for i in df.index :
                e.date, e.epoch,dg = i, i,0
                moon.compute(e)
                dg = (moon.g_ra / ephem.degree) - df.loc[i]['lahiri']
                if dg < 0 :
                     dg = 360 - abs(dg)
                arr.append(dg)
            df[pla] = arr
            return df
    return df

def getascendant(dff):
    e = ephem.Observer()
    e.lon, e.lat, e.elevation,e.horizon = '72:51:22.2', '19:01:03.4',12.40882,'-0.34'
    ltime=[]
    for i in dff.index:
        e.date, e.epoch, dg = i, i, 0
        sunrise = ephem.localtime(e.previous_rising(ephem.Sun())).replace(second=0,microsecond=0)
        min = (i - sunrise).total_seconds()/60.0
        sunn = ephem.Sun()
        e.date,e.epoch = sunrise,sunrise
        sunn.compute(e)
        sundg = (sunn.g_ra / ephem.degree) - dff.loc[i]['lahiri']
        if sundg < 0:
            sundg = 360 - abs(sundg)
        ltime.append((sundg+(min//4))%360)
    dff['asc'] = ltime
    return dff


def retromarks(dff):
    plan = {'mars':0.05,'jupiter':0.10,'saturn':0.15,'mercury':0.20,'venus':0.25,'pluto':0.3,'uranus':0.3,'neptune':0.3}
    for i in plan.keys():
        if i in dff.columns:
            dff[i+"_r"] = (dff[i] < dff[i].shift(-1))
            a= []
            for j,k in zip(dff[i+"_r"],dff.Close):
                if j == True:
                    a.append(0)
                else:
                    a.append(k+k*plan[i])
            dff[i+"_rs"] = a
    return dff


def speed_of_planet(dff):
    plan = ['mars','jupiter','saturn','mercury','venus','pluto','uranus','neptune','sun','moon']
    planets = {'moon': ephem.Moon(), 'sun': ephem.Sun(), 'mars': ephem.Mars, 'mercury': ephem.Mercury,
               'jupiter': ephem.Jupiter, 'saturn': ephem.Saturn, 'venus': ephem.Venus, 'neptune': ephem.Neptune(),
               'uranus': ephem.Uranus(), 'pluto': ephem.Pluto()}
    def hpos(body):
        return body.hlon, body.hlat
    for i in plan:
        body = planets[i]
        dic=[]
        for j in dff.index:
            dic.append(ephem.separation(hpos(body(j)), hpos(body(j+datetime.timedelta(days=1)))))
        #dff[i+"_speed"] = (dff[i] - dff[i].shift(-1))
        dff[i + "_speed"] = dic
        dff[i + "_speed"] = dff[i+"_speed"].abs()*1000
    return dff
