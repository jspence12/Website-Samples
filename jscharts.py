"""
Collection of classes and functions connecting django with charts.js

Classes included are as follows
        - Chart                     (Passes Chart-wide information to front-end. Holds Datasets. This should be the only object which interacts directly with django templates)
        - Dataset                   (Produces dataset object for front-end. This should be the only object which interacts directly with the model)
        - UniStats                  (Descriptive statistics for a single Dataset. Does not exist independently of dataset)
        - MultiStats                (Describes statistical relationships between 2 Datasets. Does not exist independently of dataset)
"""
import app
from copy import copy
from datetime import date
from django.db import models
import json


def constructDateLabels(year=1987, quarterly=False):
    """Constructs list of date labels for jschart objects"""
    labels = []
    month = 1
    while date(year,month,1) < date.today():
        if quarterly == True:
            while month <= 12 and date(year,month,1) < date.today():
                labels.append(copy(str(date(year,month,1))))
                month += 3
            month = 1
        else:
            labels.append(copy(date(year,month,1).isoformat()))
        year += 1
    if quarterly == False:
        labels = labels[:-1]
    return labels

class Chart:
    """holds jscharts datasets in a format ready to be processed into a chart"""
    __slots__ = ["name","varname","labels","datasets","stats","type","isLarge"]

    def __init__(self,name,type='bar',labels=constructDateLabels()):
        self.name = name                        #Name displayed in UI for chart
        self.varname = name.replace(" ","_")    #Variable name used in the javascript to generate the chart
        self.labels = labels                    #Values the data is broken out by
        self.datasets = []
        self.stats  =[]
        self.type = type
        self.isLarge = False

    def assignColors(self):
        """Assigns chart colors to datasets held in dataset attribute."""
        colors = [
                  '#e56f62',
                  '#8075BA',
                  '#E5D862'
                  ]
        colorIndex = 0
        if self.type == 'doughnut' or self.type == 'pie':
            #assuming only a single dataset in doughnut/pie chart
            for data in self.datasets[0]['data']:
                self.datasets[0]['backgroundColor'].append(colors[colorIndex])
                if colorIndex < len(colors):
                    colorIndex += 1
                else:
                    colorIndex = 0
        else:
            for dataset in self.datasets:
                dataset["borderColor"] = colors[colorIndex]
                dataset["backgroundColor"] = colors[colorIndex]
                if colorIndex < len(colors):
                    colorIndex += 1
                else:
                    colorIndex = 0


    def assignAxes(self):
        if self.type == 'doughnut' and self.type == 'pie':
            return
        self.datasets[0]['yAxisID']='left-axis'
        if len(self.datasets) > 1:
            for dataset in self.datasets[1:]:
                dataset['yAxisID']='right-axis'

    def getStats(self,dataset0,dataset1=None):
        if dataset1 is None:
            self.stats.append(UniStats(dataset0))
        else:
            self.stats.append(MultiStats(dataset0,dataset1))

    def jsonEncode(self):
        """encodes datasets for template"""
        self.datasets = json.dumps(self.datasets)


class Dataset:
    """
    Generates single-metric/calculation datasets to be stored in a chart object's datasets list attribute.
    The bar_graph(), line_graph(), or pie_chart() Dataset methods should be invoked when appending/extending
    Dataset objects to a chart object's dataset attribute.
    
    Labels should be inherited from the parent chart object so that all datasets in a single chart are displayed
    in a consistent manner.
    """
    __slots__ = ["verboseName","labels","labelsColumn","dataDict","data"]

    def __init__(self, querySet, metric, labels=constructDateLabels(), labelsColumn='date'):
        self.verboseName = self._getVerboseName(querySet, metric)
        self.labelsColumn = labelsColumn
        self.labels = labels
        self.dataDict = self._createDataDict()
        self.data = self._parseQuerySet(querySet, metric, labelsColumn)

    def _parseQuerySet(self, querySet, metric, labelsColumn):
        """Translates django QuerySet into dataset-compatible dataset"""
        if callable(getattr(querySet[0],metric)):
            for row in querySet:
               self.dataDict[str(getattr(row,self.labelsColumn))] = eval('row.%s()' % metric)['value']
        else:
            for row in querySet:
                if getattr(row,metric) is not None:
                    self.dataDict[str(getattr(row,self.labelsColumn))] = float(getattr(row,metric))
        return self._createDataList()

    def _getVerboseName(self, querySet, metric):
        if callable(getattr(querySet[0],metric)):
            return eval('querySet[0].%s()' % metric)['verbose_name']
        else:
            return querySet.model._meta.get_field(metric).verbose_name

    def _createDataDict(self):
        """Creates empty dictionary to store data values in."""
        dataDict = {}
        for name in self.labels:
            dataDict[name] = None
        return dataDict

    def _createDataList(self):
        """Helper function for _parseQuerySet Method. Returns ordered data list from values stored in dataDict"""
        data= []
        for name in self.labels:
            data.append(self.dataDict[name])
        return data

    def bar_graph(self):
        """Returns bar graph dataset compatible with chart objects of type 'bar'."""
        return {"label":self.verboseName,"borderColor":None, "backgroundColor":None,"data":self.data}
    
    def line_graph(self):
        """Returns line graph dataset compatible with chart objects of type 'line' or 'bar'."""
        return {"label":self.verboseName,"borderColor":None, "backgroundColor":None,"fill":False,"data":self.data ,"type":"line","lineTension":0}

    def pie_graph(self):
        """Returns dataset compatible with chart objects of type 'pie' or 'donut'."""
        return {'label':self.verboseName, 'data':self.data, 'backgroundColor':[]}


class CrossDataset:
    """
    Class specifically for comparing aggregate totals for separate metrics in a pie chart
    line_graph and bar_graph methods are unimplemented for this class as normal Dataset class should 
    be able to handle those cases.

    It is the querySet's responsibility to ensure the desired scope of aggregation
    """
    def __init__(self, querySet, name, labels):
        self.name = name
        self.data = self._getData(querySet,labels)

    def _getData(self,querySet,labels):
        """Generates aggregated dataset from a QuerySet or single Model instance"""
        aggList = []
        if isinstance(querySet,models.QuerySet):
            for metric in labels:
                aggList.append(0)
            for row in querySet:
                aggIndex=0
                for metric in labels:
                    if callable(getattr(row,metric)):
                        aggList[aggIndex] = eval('row.%s()' % metric)
                    elif getattr(row,metric) is not None:
                        aggList[aggIndex] += float(getattr(row,metric))
                    aggIndex += 1
        else:
            # assumes single Model instance
            for metric in labels:
                if callable(getattr(querySet,metric)):
                    aggList[aggIndex] = eval('querySet.%s()' % metric)
                elif getattr(querySet,metric) is not None:
                    aggList.append(float(getattr(querySet,metric)))
        return aggList

    def pie_graph(self):
        return {'label':self.name, 'data':self.data, 'backgroundColor':[]}


class UniStats:
    """Univariate statistics for a jscharts dataset"""
    __slots__ = ["_data","sum","sumSquare","mean","variance","stDev"]

    def __init__(self,dataset):
        self._data = self._sanitizeData(dataset)
        self.sum = self._getSum()
        self.sumSquare = self._getSumSquare()
        self.mean = self._getMean()
        self.variance = self._getVariance()
        self.stDev = self._getStDev()

    def _sanitizeData(self,dataset):
        """removes nulls from dataset dataset for calculations"""
        sanitizedData = []
        if isinstance(dataset,dict):
            dataset = dataset['data']
        else:
            dataset = dataset.data
        for value in dataset:
            if value != None:
                sanitizedData.append(value)
        return sanitizedData

    def _getSum(self):
        """Calculates sum of dataset"""
        sum = 0
        for value in self._data:
            sum += value
        return sum

    def _getSumSquare(self):
        """Calculates sum of squares of dataset"""
        sumSquare = 0
        for value in self._data:
            sumSquare += (value ** 2)
        return sumSquare

    def _getMean(self):
        """Generates mean summary statistic for self"""
        return self.sum / len(self._data)

    def _getVariance(self):
        """Generates variance summary statistic for self"""
        variance = ((self.sumSquare / (len(self._data))) - self.mean)
        return variance

    def _getStDev(self):
        """Generates standard deviation summary statistic for self"""
        return self.variance ** .5


class MultiStats:
    """Multivariate statistics comparing 2 jscharts datasets"""
    __slots__ = ['names','_likeDataSets','means','sums','sumSquares','variances','stDevs','covariance','correlation']

    def __init__(self,dataset0,dataset1):
        self.names = (dataset0["label"],dataset1["label"])
        self._likeDataSets = self._getLikeDataSet(dataset0["data"],dataset1["data"])
        if len(self._likeDataSets) == 0:
            self.sums = None
            self.sumSquares = None
            self.means = None
            self.variances = None
            self.stDevs = None
            self.covariance = None
            self.correlation = None
        else:
            self.sums = self._getSums()
            self.sumSquares = self._getSumSquares()
            self.means = self._getMeans()
            self.variances = self._getVariances()
            self.stDevs = self._getStDevs()
            self.covariance = self._getCovariance()
            self.correlation = self._getCorrelation()

    def _validateLikeness(self,labels0,labels1):
        """Validates that we are comparing like datasets between 2 jscharts objects"""
        for label0,label1 in zip(labels0,labels1):
            if label0 != label1:
                raise ValueError("Labels from both jschart objects must be identical")
        return
    
    def _getLikeDataSet(self,data0,data1):
        """Returns list of tuples for all labels in which both jsChart objects hold data."""
        sharedSet = []
        for val0, val1 in zip(data0,data1):
            if val0 != None and val1 != None:
                sharedSet.append((val0,val1))
        return sharedSet

    def _getSums(self):
        """Generates tuple of sums for overlapping data between input jsChart objects"""
        sum0 = 0
        sum1 = 0
        for val0, val1 in self._likeDataSets:
            sum0 += val0
            sum1 += val1
        return (sum0,sum1)

    def _getSumSquares(self):
        """Generates tuple of sums for squares of overlapping data between input jsChart objects"""
        sumSquare0 = 0
        sumSquare1 = 0
        for val0, val1 in self._likeDataSets:
            sumSquare0 += val0 ** 2
            sumSquare1 += val1 ** 2
        return (sumSquare0, sumSquare1)

    def _getMeans(self):
        """Generates tuple of means for overlapping data between input jsChart objects"""
        mean0 = self.sums[0]/len(self._likeDataSets)
        mean1 = self.sums[1]/len(self._likeDataSets)
        return (mean0, mean1)

    def _getVariances(self):
        """Generates tuple of variances for overlapping data between input jsChart objects"""
        variance0 = (self.sumSquares[0])/len(self._likeDataSets) - (self.means[0] ** 2)
        variance1 = (self.sumSquares[1])/len(self._likeDataSets) - (self.means[1] ** 2)
        return (variance0,variance1)
    
    def _getStDevs(self):
        """Generates tuple of standard deviations for overlapping data between input jsChart objects"""
        stDev0 = self.variances[0] ** .5
        stDev1 = self.variances[1] ** .5
        return (stDev0,stDev1)

    def _getCovariance(self):
        """Calculates covariance summary statistic"""
        sumProduct = 0
        for val0, val1 in self._likeDataSets:
            sumProduct += (val0 - self.means[0]) * (val1 - self.means[1])
        covariance = sumProduct / (len(self._likeDataSets) - 1)
        return covariance

    def _getCorrelation(self):
        """Calculates correlation summary statistic"""
        sumProduct = 0
        N = len(self._likeDataSets)
        for val0,val1 in self._likeDataSets:
            sumProduct += val0 * val1
        correlation = (N * sumProduct) - (self.sums[0] * self.sums[1])
        correlation /= (((N * self.sumSquares[0]) - (self.sums[0] ** 2)) * ((N * self.sumSquares[1]) - (self.sums[1] ** 2))) ** .5
        return correlation