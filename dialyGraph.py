import graphGen as gg
import datetime as dt

def dailyGraph(period = 'monthly'):
    # Set timeframe
    end = dt.datetime.now()
    if period == 'weekly':
        start = end - dt.timedelta(days=7)
    else:
        start = end - dt.timedelta(days=30)

    # Open database connection
    dbRsam = gg.Database('rsam')
    dbSeismisitas = gg.Database('seismisitas')
    dbGps = gg.Database('gps')
    dbDoas = gg.Database('doas')

    # Create RSAM graph and save it
    plotRsam = gg.ploter()
    dataRsam = dbRsam.fetch('PASB', start, end)
    plotRsam.append(dataRsam)
    plotRsam.generate('RSAM', period)

    # Create Seismisity graph and save it
    num2Seismis = {0: 'VTA', 1: 'VTB', 2: 'MP', 3: 'RF', 4: 'DG', 5: 'TT'}
    plotSeismis = gg.ploter()
    for i in xrange(0,len(num2Seismis)):
        dataSeismis = dbSeismisitas.fetch(num2Seismis[i], start, end)
        plotSeismis.append(dataSeismis)
    plotSeismis.generate('SEISMISITAS', period)

    # Create GPS graph and save it
    gpsPair = {0: ['PASB', 'BABA'],
            1: ['PASB', 'DELS'],
            2: ['PASB', 'KLAT'],
            3: ['PASB', 'GRWH'],
            4: ['PLAW', 'PASB']}
    plotGps = gg.ploter()
    for i in xrange(0,len(gpsPair)):
        dataGps = dbGps.fetch(gpsPair[i], start, end)
        plotGps.append(dataGps)
    plotGps.generate('GPS', period)

    # Create DOAS graph and save it
    plotDoas = gg.ploter()
    dataDoas = dbDoas.fetch('BABA', start, end)
    plotDoas.append(dataDoas)
    plotDoas.generate('DOAS', period)

dailyGraph(period = 'weekly')
dailyGraph(period = 'monthly')