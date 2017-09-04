from pytrends.request import TrendReq
from IPython.core.pylabtools import figsize
import numpy as np
import pandas as pd 
import matplotlib as mpl
import matplotlib.pyplot as plt
result = pd.DataFrame

# Define date to string formatter function
def date_to_string(datetimevar, form='%Y-%m-%dT%H'):
    import pandas as pd 
    t = pd.to_datetime(str(datetimevar)) 
    timestring = t.strftime(form)
    return timestring

def collect_data(start_date = '2017-07-01T00:00', end_date = 'now'):
    from pytrends.request import TrendReq
    from IPython.core.pylabtools import figsize
    import numpy as np
    import pandas as pd 
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    
    # Set start date for data collection (end date is today)
    start_date = np.datetime64(start_date)
    end_date = np.datetime64(end_date)
    end_date = np.datetime64(date_to_string(end_date, form='%Y-%m-%d'))

    # Count number of days between start and end
    delta_days = np.timedelta64(end_date - start_date, 'D')
    delta_days = (delta_days/np.timedelta64(1, 'D')).astype(int)
    print("Number of days: ", delta_days+1)

    # Login to Google. Only need to run this once, the rest of requests will use the same session.
    pytrend = TrendReq()
    for i in range(0,delta_days+1):
        
        start = start_date+np.timedelta64(i, 'D')
        finish = start_date+np.timedelta64(i+5, 'D')
        
        # reformat to Google format
        start = date_to_string(start)
        finish = date_to_string(finish, form='%Y-%m-%dT12') #extend finish time by 12 hours for sufficient overlap to calibrate chain
        chainlink = i
        print("start: ", start, "finish: ", finish, "chainlink: ", chainlink)
        pytrend.build_payload(kw_list=['vote yes', 'vote no'], timeframe='%s %s' % (start, finish), geo='AU')
        interest_over_time_df = pytrend.interest_over_time()
        interest_over_time_df["chainlink"] = chainlink
        if i==0:
            result = interest_over_time_df
        else:
            result = pd.concat([result, interest_over_time_df])
    result = result.sort_index()
    return result

def scale_data(result):
    # initialise scalar array for storying chain scalars
    chainlength = max(result.chainlink)
    print("Chain length = ", chainlength)
    scalar = np.zeros((chainlength+1,2))
    scalar[0,] = 1
    
    # initialise scalar column
    result['scalar'] = 1

    # initialise scalar array for storying chain scalars
    scalar = np.zeros((chainlength+1,2))
    scalar[0,] = 1
    for i in range(1,chainlength):
        # filter each adjacent chainlink to overlapping time slots
        chain_a = result[result['chainlink']==i-1]
        chain_b = result[result['chainlink']==i]
        chain_a_filter = chain_a[chain_a.index.isin(chain_b.index)]
        chain_b_filter = chain_b[chain_b.index.isin(chain_a.index)]        

        # calculate means of scalars on both 'vote yes' and 'vote no' searches
        yes_prop = (chain_a_filter['vote yes'] / chain_b_filter['vote yes'])
        no_prop = (chain_a_filter['vote no'] / chain_b_filter['vote no'])
        yes_prop[np.isinf(yes_prop)] = np.nan
        no_prop[np.isinf(no_prop)] = np.nan
        props = pd.concat([yes_prop, no_prop])

        #store scalars in scalar array
        scalar[i,0] = props.mean()
    
    # normalize scalars to index    
    scalar[0,1] = 100
    for i in range(1,chainlength):
        scalar[i,1] = scalar[i-1,1] * scalar[i,0]
    
    # Normalize data frame
    for i in range(chainlength):
        result.ix[result.chainlink==i, 'normalized_yes'] = result.ix[result.chainlink==i, 'vote yes'] * scalar[i,1]
        result.ix[result.chainlink==i, 'normalized_no'] = result.ix[result.chainlink==i, 'vote no'] * scalar[i,1]
    result = result.dropna(axis=0, how='any', subset=['normalized_yes', 'normalized_no'])
    result = result.sort_index()
    result = result[~result.index.duplicated(keep='first')]
    return result

def group_by_day(result_filtered):
    result_filtered_grouped = pd.groupby(result_filtered,by=[result_filtered.index.year, result_filtered.index.month, result_filtered.index.day]).sum()
    result_filtered_grouped = result_filtered_grouped.reset_index()
    result_filtered_grouped['date'] = pd.to_datetime(result_filtered_grouped['level_0'] * 10000 + result_filtered_grouped['level_1'] * 100 + result_filtered_grouped['level_2'], format="%Y%m%d")
    result_filtered_grouped.set_index('date', inplace=True)
    result_filtered_grouped['odds'] = result_filtered_grouped['normalized_yes'] / (result_filtered_grouped['normalized_yes'] + result_filtered_grouped['normalized_no'])
    result_filtered_grouped['odds_rolling'] = pd.rolling_mean(result_filtered_grouped['odds'], 7)
    return result_filtered_grouped

def join_data(first, second):
    first_filter = first[first.index.isin(second.index)]
    second_filter = second[second.index.isin(first.index)]        
    first_filter.sort_index()
    second_filter.sort_index()
    # calculate means of scalars on both 'vote yes' and 'vote no' searches
    yes_prop = (first_filter['normalized_yes'] / second_filter['normalized_yes'])
    no_prop = (first_filter['normalized_no'] / second_filter['normalized_no'])
    yes_prop[np.isinf(yes_prop)] = np.nan
    no_prop[np.isinf(no_prop)] = np.nan
    props = pd.concat([yes_prop, no_prop])
    scalar = props.mean()
    second['normalized_yes'] = second['normalized_yes'] * scalar
    second['normalized_no'] = second['normalized_no'] * scalar
    joined = first.append(second)
    joined = joined.dropna(axis=0, how='any', subset=['normalized_yes', 'normalized_no'])
    joined = joined.sort_index()
    joined = joined[~joined.index.duplicated(keep='first')]
    joined['yes_rolling'] = pd.rolling_mean(joined['normalized_yes'], 12) #12 hour rolling avg
    joined['no_rolling'] = pd.rolling_mean(joined['normalized_no'], 12) #12 hour rolling avg
    return joined