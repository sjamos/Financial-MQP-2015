from datetime import date

class FinanceData:
    datapoints = None
    
    def __init__(self):
        self.datapoints = []
    
    def insertPoint(self, aDate, aOpenPrice, aHighPrice, aLowPrice, aClosePrice, aVolume):
        point = {}
        
        if isinstance(aDate, date):
            point['DATE'] = aDate
        else:
            point['DATE'] = None
        
        point['OPEN'] = float(aOpenPrice)
        point['HIGH'] = float(aHighPrice)
        point['LOW'] = float(aLowPrice)
        point['CLOSE'] = float(aClosePrice)
        point['VOL'] = int(aVolume)
        
        self.datapoints.append(point)
    