# -*- coding: utf-8 -*-
"""
Created on Tue May 22 17:11:14 2018

@author: brady
"""

from sqlalchemy import create_engine
import pandas as pd 
import datetime
import numpy as np

def Connection(name):
    '''Create a connection to database'''
    db_type = 'sqlite'
    conn_string = db_type + ':///' + name
    engine = create_engine(conn_string)
    return engine 

def CorrectDate(df, date):
    '''Dates are stored as integers in database. Provided a dataframe and the 
    name of the date column, this function returns a data frame with a 
    correctly formatted date column'''
    
    df[date] = df[date].apply(lambda time: \
      datetime.datetime.fromtimestamp(time/1e3))
    return df

def Bodyweight(name):
    '''Get bodyweight data over time from database and return correctly 
    formatted dataframe'''
    engine = Connection(name)
    bw_df = pd.read_sql_query("SELECT * FROM Bodyweight", engine, \
                              index_col="id")
    
    #Correctly format date data and weight data
    bw_df = CorrectDate(bw_df, 'Time')
    bw_df['Weight'] = bw_df['Weight'].apply(lambda weight: round(weight, 1))
    
    #Rename columns to more accurate identifier, and set date as index
    bw_df = bw_df.rename(index = str, columns={'Time':'Date', \
                                               'Weight':'Weight (kg)'})
    bw_df = bw_df.set_index('Date')
    
    return bw_df

def ProgramLog(db_name):
    '''Create dating frame containing all exercise data indexed by date and 
    exercise'''
    
    #Connect to database and query to obtain wanted data
    engine = Connection(db_name)
    query = ("SELECT S.Comment AS Set_Comment, S.Weight, S.Rep, S.RPE, "
         "EN.Name AS Exercise, ED.Comment AS Exercise_Comment, D.Time AS Date "
         "FROM [Set] AS S "
         "INNER JOIN ExerciseName AS EN ON S.fkExerciseNameID=EN.id "
         "INNER JOIN ExerciseDay AS ED ON S.fkExerciseDayID=ED.id "
         "INNER JOIN Day AS D ON ED.fkDayID=D.id")
    programLog = pd.read_sql_query(query, engine)
    
    #Format and tidy dataframe
    programLog = CorrectDate(programLog, 'Date')
    programLog = programLog[['Date', 'Exercise', 'Rep', 'Weight', 'RPE', \
                             'Exercise_Comment', 'Set_Comment']]
    programLog = programLog.set_index(['Date', 'Exercise']).sort_index()
    
    return programLog

def LoadRpeChart(path='RPE_CHART.csv'):
    '''Load RPE reference chart from file'''
    return pd.read_csv(path, index_col=0)

def RpeToPercent(reps, rpe, path='RPE_CHART.csv'):
    '''Using a value for reps and rpe, use rpe chart to look a percentage of 
    one rep max value'''
    chart = LoadRpeChart(path)
    #Note: Value for reps take the column headers and are stored as strings
    return chart.loc[float(rpe), str(reps)]

def KgPlatesRound(weight):
    '''Round to smallest (most-common) denomination of kg plates'''
    return(2.5 * round(weight / 2.5))
    
def Est1RM(weight, reps, rpe, path='RPE_CHART.csv'):
    '''Using look up chart estimate a one-rep max based on the weight, 
    reps and RPE of the set'''
    
    #Dummy variables for checks
    REPS = reps
    RPE = rpe
    
    #The following is take into account the ranges that appear on the chart
    if RPE < 5.0:
        RPE = 5.0
    if REPS > 12:
        REPS = 12
    
    if REPS != 0:
        percent = RpeToPercent(REPS, RPE, path)
        estimate = KgPlatesRound(weight / percent)
        return estimate
    
    #This is to take into account the case a set with zero reps 
    else:
        return None

def HighWeight(group):
    '''Return row that contains highest value of weight in a group'''
    return group.loc[group['Weight'] == group['Weight'].max()]

def Est1RMLog(log):
    '''Given a program return a dataframe containing estimated 1 rep max data 
    over time for each exercise in the log'''
    
    #Select only sets with one or more reps
    #Get the row containing the highest weight for each date and exercise
    highWeight_Log = log[log['Rep'] >= 1].groupby(['Date', 'Exercise'])\
    [['Rep', 'Weight', 'RPE', 'Est_1rm']].apply(HighWeight)
    
    #Get the highest value for estimated one rep max for each date/exercise
    maxLog = highWeight_Log.groupby(['Date', 'Exercise'])[['Est_1rm']].agg('max')
    
    #Swap index level so can see how each exercise develops over time
    maxLog = maxLog.swaplevel(0,1).sort_index()
    return maxLog
    

