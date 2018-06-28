# -*- coding: utf-8 -*-
"""
Created on Mon May 28 09:19:44 2018

@author: My Computer
"""

import os
import MySQLdb
import logging
import datetime as dt
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mtpDate
import numpy as np
import pandas as pd
import locale
from matplotlib.ticker import MaxNLocator

logger = logging.getLogger('MerapiData')

try:
    locale.setlocale(locale.LC_ALL, 'id_ID.UTF-8')
except:
    locale.setlocale(locale.LC_ALL, 'Indonesian_indonesia.1252')
os.environ['TZ'] = 'Asia/Jakarta'


DATABASE = {
    'rsam': {
        'host': '192.168.5.74',
        'port': 3306,
        'user': 'merapi1',
        'passwd': 'merapi',
        'db': 'rsam_wovodat'
    },
    'seismisitas': {
        'host': '192.168.5.74',
        'port': 3306,
        'user': 'merapi1',
        'passwd': 'merapi',
        'db': 'seismic_bulletin'
    },
    'gps': {
        'host': '192.168.5.74',
        'port': 3306,
        'user': 'merapi1',
        'passwd': 'merapi',
        'db': 'wogps'
    },
    'doas': {
        'host': '192.168.5.74',
        'port': 3306,
        'user': 'merapi1',
        'passwd': 'merapi',
        'db': 'doas'
    },
    'energi': {
        'host': '192.168.5.74',
        'port': 3306,
        'user': 'merapi1',
        'passwd': 'merapi',
        'db': 'seismic_bulletin'
    }
}

# RSAM Station ID
RsamStatCode = {'PASB': 75, 'MERB': 74, 'LABB': 73, 'KLAS': 72,
                'IJOB': 68, 'KLAB': 71, 'JRJB': 70, 'IMOB': 69,
                'DELS': 66, 'GRAB': 67, 'PLAS': 76, 'PUSS': 77, }

RsamStations = ['PASB', 'MERB', 'LABB', 'KLAS',
                'IJOB', 'KLAB', 'JRJB', 'IMOB',
                'DELS', 'GRAB', 'PLAS', 'PUSS']

# Seismisitas event type
EventType = ['VTA', 'VTB', 'MP', 'RF', 'DG', 'TT']

# Seismisitas dict
EVENTTYPE = {'VTA': 'VTA', 'VTB': 'VTB', 'MP': 'MP', 'RF': 'ROCKFALL', 'DG': 'GASBURST', 'TT': 'TECT'}

# GPS baseline
GpsBaseline = ['PASB-BAB', 'PAS-DEL', 'PAS-KLA', 'PAS-JGR', 'PLA-PAS']

# GPS station
GpsStation = ['BABA', 'BPTK', 'DELS', 'GRWH', 'JRAK'
              'KLAT', 'KNDT', 'PASB', 'PLAW', 'SELO']


class DataPacket:
    """docstring for DataPacket"""
    def __init__(self, data, parameter, station, start, end):
        self.parameter = parameter
        self.station = station
        self.value = []
        self.time = []
        self.start = start
        self.end = end
        for datum in data:
            self.value.append(datum['VALUE'])
            self.time.append(datum['TIME'])

    def getCumulative(self):
        value = np.array(self.value)
        cumulative = np.cumsum(value)
        return cumulative.tolist()

class Database:
    def __init__(self, parameter):
        self.parameter = parameter
        self.conn = MySQLdb.connect(**DATABASE[parameter])
        self.cursor = self.conn.cursor()

    def _del_(self):
        self.conn.close()

    def execute(self, query):
        try:
            cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query)
            return cursor.fetchall()
        except MySQLdb.Error as e:
            logger.error('Error {0}: {1}'.format(e.args[0], e.args[1]))

    def fetch(self, station, start, end):
        if self.parameter == 'rsam':
            return self.fetchRSAM(station, start, end)
        elif self.parameter == 'seismisitas':
            return self.fetchSeismisitas(station, start, end)
        elif self.parameter == 'gps':
            return self.fetchGPS(station[0], station[1], start, end)
        elif self.parameter == 'doas':
            return self.fetchDOAS(start, end)
        else:
            print 'parameter error'


    def fetchExplosions(self, start, end):
        query = r"""
                SELECT eventdate
                FROM bulletin
                WHERE eventdate >= '{start}'
                AND eventdate <= '{end}'
                AND type = 'EXPLOSION'
                GROUP BY DATE_FORMAT(eventdate, "%Y-%m-%d %H:%i:%s");
                """
        data = self.execute(query.format(
            start=start.strftime('%Y-%m-%d %H:%M:%S'),
            end=end.strftime('%Y-%m-%d %H:%M:%S')))
        dataList = []
        for i in range(0, len(data)):
            dataList.append(data[i]['eventdate'])
        return dataList


    def fetchEnergi(self, start, end):
        query = r"""
            SELECT *

            """

    def fetchRSAM(self, station, start, end):
        query = r"""
            SELECT sd_rsm_stime AS TIME, sd_rsm_count AS VALUE
            FROM sd_rsm
            WHERE sd_rsm_stime >= '{start}'
            AND sd_rsm_stime <= '{end}'
            AND sd_sam_id = {stat};
            """
        data = self.execute(query.format(
            start=start.strftime('%Y-%m-%d %H:%M:%S'),
            end=end.strftime('%Y-%m-%d %H:%M:%S'),
            stat=RsamStatCode[station]
        ))
        return DataPacket(data, 'rsam', station, start, end)


    def binCounter(self, data, start, end):
        dates = []
        for datum in data:
            dates.append(datum['DATE'])
        data = pd.DataFrame()
        data['date'] = dates
        datas = []
        dataGrouped = data.groupby('date')['date'].count()
        for i in range(0, (end - start).days):
            newDate = start + dt.timedelta(days=i)
            try:
                value = dataGrouped[newDate.strftime('%Y-%m-%d')]
            except:
                value = 0
            datum = {'TIME': newDate, 'VALUE': value}
            datas.append(datum)
        return datas


    def fetchSeismisitas(self, eventType, start, end):
        query = r"""
            SELECT DATE_FORMAT(eventdate, "%Y-%m-%d") AS DATE
            FROM bulletin
            WHERE eventdate >= '{start}'
            AND eventdate <= '{end}'
            AND type = '{eventType}'
            GROUP BY DATE_FORMAT(eventdate, "%Y-%m-%d %H:%i:%s");
        """
        data = self.execute(query.format(
            start=start.strftime('%Y-%m-%d %H:%M:%S'),
            end=end.strftime('%Y-%m-%d %H:%M:%S'),
            eventType=EVENTTYPE[eventType]))
        data = self.binCounter(data, start, end)
        return DataPacket(data, 'seismisitas', eventType, start, end)


    def fetchDOAS(self, start, end):
        query = r"""
            SELECT DATE AS TIME, VALUE
            FROM BAB
            WHERE DATE >= '{start}'
            AND DATE <= '{end}'
            ORDER BY DATE ASC;
            """
        data = self.execute(query.format(
            start=start.strftime('%Y-%m-%d %H:%M:%S'),
            end=end.strftime('%Y-%m-%d %H:%M:%S')))
        return DataPacket(data, 'doas', 'BABA', start, end)


    def fetchGpsRecord(self, station, start, end):
        query = r"""
            SELECT DATE_FORMAT(DATE, "%Y-%m-%d") AS DATE,
                EAST,
                NORTH,
                UP
            FROM {station}
            WHERE DATE >= '{start}'
            AND DATE <= '{end}'
            """
        data = self.execute(query.format(
            station=station,
            start=start.strftime('%Y-%m-%d %H:%M:%S'),
            end=end.strftime('%Y-%m-%d %H:%M:%S')))
        return data


    def getGpsBaseline(self, dataA, dataB):
        dfA = pd.DataFrame(list(dataA))
        dfB = pd.DataFrame(list(dataB))
        data = pd.merge(dfA, dfB, on='DATE', how='outer')
        data = data.dropna(axis=0, how='any')
        data.columns = ['DATE', 'eA', 'nA', 'uA', 'eB', 'nB', 'uB']
        data.loc[:, 'BASELN'] = np.sqrt((data.eA - data.eB)**2 +
                                (data.nA - data.nB)**2 +
                                (data.uA - data.uB)**2).loc[:]
        data = data.drop(['eA', 'nA', 'uA', 'eB', 'nB', 'uB'], axis=1)
        data['DATE'] = pd.to_datetime(data['DATE'])
        #data = data.set_index('DATE')
        value = data.BASELN.tolist()
        date = data.DATE.tolist()
        data = []
        for i in xrange(len(value)):
            datum = {'TIME': date[i], 'VALUE': value[i]}
            data.append(datum)
        return data


    def fetchGPS(self, stationA, stationB, start, end):
        dataA = self.fetchGpsRecord(stationA, start, end)
        dataB = self.fetchGpsRecord(stationB, start, end)
        return DataPacket(self.getGpsBaseline(dataA, dataB), 'gps', stationA + '-' + stationB, start, end)


class ploter:
    def __init__(self):
        self.dataPackets = []


    def append(self, datapacket):
        self.dataPackets.append(datapacket)
        
    
    def generate(self, title, period = 'monthly'):
        if len(self.dataPackets) == 0:
            return

        # Define start and end
        start = self.dataPackets[0].start
        end = self.dataPackets[0].end
        
        # Fetch Explosion data as list
        dbSeismik = Database('seismisitas')
        explosions = dbSeismik.fetchExplosions(start, end)
        ndata = len(self.dataPackets)
        if ndata == 0:
            return

        # Generate figure and axes    
        fig, ax = plt.subplots(ndata, 1, sharex = True, figsize=(12, 6))
        if(ndata == 1):
            ax = [ax]
        fig.tight_layout()
        fig.autolayout = True

        for i in range(0,ndata):
            data = self.dataPackets[i]

            # Draw alert level background for every axes
            ax[i].axvspan(start, dt.datetime(2018,05,21,23), alpha=0.5, color='lime')
            ax[i].axvspan(dt.datetime(2018,05,21,23),end, alpha=0.5, color='yellow')
        
            # check 
            if(not not explosions):
                if i == 0:
                    pass
                    letusan = ax[i].axvline(explosions[0],linewidth=2, color='r',label='Letusan')
                # Draw vertcaline explosion for every axes
                for explosion in explosions:
                    ax[i].axvline(explosion,linewidth=2, color='r')

            # Extract from datapacket
            if data.parameter == 'rsam':
                ax[i].plot(data.time, data.value,'ko-', label = data.parameter)
                # Draw RSAM Cumulative axis
                axT = ax[i].twinx()  # instantiate a second axes that shares the same x-axis
                axT.plot(data.time, data.getCumulative(),'ro-', label = 'kumulatif')

            elif data.parameter == 'doas':
                ax[i].plot(data.time, data.value,'ko', label = data.parameter)

            elif data.parameter == 'gps':
                ax[i].plot(data.time, data.value,'ko-', label = data.parameter)

            elif data.parameter == 'seismisitas':
                ax[i].bar(data.time, data.value, label = data.parameter)

            ax[i].set_ylabel(data.station)
            ax[i].legend(loc=1)

            # Setting necessary figure param
            if(i==0):
                ax[i].set_title(title,fontsize=15)
            ax[i].xaxis_date()
            ax[i].xaxis.label.set_visible(False)
            ax[i].yaxis.set_major_locator(MaxNLocator(4))
            ax[i].set_xlim(start,end)
            
        ax[ndata-1].xaxis.set_major_locator(mtpDate.MonthLocator())
        ax[ndata-1].xaxis.set_major_formatter(mtpDate.DateFormatter('\n%B %Y'))
        ax[ndata-1].xaxis.set_minor_locator(mtpDate.DayLocator())
        ax[ndata-1].xaxis.set_minor_formatter(mtpDate.DateFormatter('%d'))

        # Show Legend
        plt.sca(ax[0])
        
        # Save figure to images
        try:
            directory = '/home/merapi/Storage/grafik/' + end.strftime('%Y-%m-%d') + '/'
            if(period == 'monthly'):
                plt.savefig(directory+'30hr/'+data.parameter+'-'+end.strftime('%Y-%m-%d-%H:%M:%S')+'-'+'30hr'+'.png', bbox_inches="tight")
            elif(period == 'weekly'):
                plt.savefig(directory+'7hr/'+data.parameter+'-'+end.strftime('%Y-%m-%d-%H:%M:%S')+'-'+'7hr'+'.png', bbox_inches="tight")

        except:
            if(period == 'monthly'):
                plt.savefig(data.parameter+'-'+'30hr'+'.png', bbox_inches="tight")
            elif(period == 'weekly'):
                plt.savefig(data.parameter+'-'+'7hr'+'.png', bbox_inches="tight")

# def dailyGraph(period = 'monthly'):
#     # Set timeframe
#     end = dt.datetime.now()
#     if period == 'weekly':
#         start = end - dt.timedelta(days=7)
#     else:
#         start = end - dt.timedelta(days=30)

#     # Open database connection
#     dbRsam = Database('rsam')
#     dbSeismisitas = Database('seismisitas')
#     dbGps = Database('gps')
#     dbDoas = Database('doas')

#     # Create RSAM graph and save it
#     plotRsam = ploter()
#     dataRsam = dbRsam.fetch('PASB', start, end)
#     plotRsam.append(dataRsam)
#     plotRsam.generate('RSAM', period)

#     # Create Seismisity graph and save it
#     num2Seismis = {0: 'VTA', 1: 'VTB', 2: 'MP', 3: 'RF', 4: 'DG', 5: 'TT'}
#     plotSeismis = ploter()
#     for i in xrange(0,len(num2Seismis)):
#         dataSeismis = dbSeismisitas.fetch(num2Seismis[i], start, end)
#         plotSeismis.append(dataSeismis)
#     plotSeismis.generate('SEISMISITAS', period)

#     # Create GPS graph and save it
#     gpsPair = {0: ['PASB', 'BABA'],
#             1: ['PASB', 'DELS'],
#             2: ['PASB', 'KLAT'],
#             3: ['PASB', 'GRWH'],
#             4: ['PLAW', 'PASB']}
#     plotGps = ploter()
#     for i in xrange(0,len(gpsPair)):
#         dataGps = dbGps.fetch(gpsPair[i], start, end)
#         plotGps.append(dataGps)
#     plotGps.generate('GPS', period)

#     # Create DOAS graph and save it
#     plotDoas = ploter()
#     dataDoas = dbDoas.fetch('BABA', start, end)
#     plotDoas.append(dataDoas)
#     plotDoas.generate('DOAS', period)

# dailyGraph(period = 'weekly')
# dailyGraph(period = 'monthly')