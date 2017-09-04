import pulse
import pandas as pd
import numpy as np
result = pd.read_pickle('result.pd')
maxtime = np.datetime64(max(result.index))
now = np.datetime64('now')
delta = pd.Timedelta(now - maxtime, 'h')
print(delta.seconds)
if (delta.days > 0 or delta.seconds >= 3600):
    result2 =  pulse.collect_data(start_date=maxtime-pd.Timedelta('2 day'))
    result2 = pulse.scale_data(result2)
    result = pulse.join_data(result, result2)
    print("Updated data")
    result.to_pickle('result.pd')
    result.to_csv('result.csv')
    pulse.group_by_day(result).to_csv('result-grouped.csv')
else:
    print("No update required")
    result.to_pickle('result.pd')
    result.to_csv('result.csv')
    pulse.group_by_day(result).to_csv('result-grouped.csv')