import pulse
import pandas as pd
import numpy as np
import json

result = pd.read_pickle('result.pd')
maxtime = np.datetime64(max(result.index))
now = np.datetime64('now')
delta = pd.Timedelta(now - maxtime, 'h')
print(delta.seconds)

result2 =  pulse.collect_data(start_date=maxtime-pd.Timedelta('7 day'))
result2 = pulse.scale_data(result2)
result = pulse.join_data(result, result2)
print("Updated data")

# Pickle and write data
result.to_pickle('result.pd')
result.to_csv('result.csv', float_format='%g', encoding='utf-8')
result_grouped = pulse.group_by_day(result)
result_grouped.to_csv('result-grouped.csv',float_format='%g', encoding='utf-8')

# Calculate and save metrics
last_update = pulse.date_to_string(now+np.timedelta64(10, 'h'), form="%d/%m/%y %H:%M") # make pretty string

current_hour = max(result.index.values) # get latest date
current_hour = current_hour+np.timedelta64(12, 'h') # add 12h for Sydney time
current_hour = pulse.date_to_string(current_hour, form="%d/%m/%y %H:%M") # make pretty string

last_hour = pulse.collect_last_hour()
last_hour = "{0:.0f}%".format(last_hour * 100)

last_day = pulse.collect_last_day()
last_day = "{0:.0f}%".format(last_day * 100)

rolling_7_days = result_grouped.iloc[-1]['odds_rolling']
rolling_7_days = "{0:.0f}%".format(rolling_7_days * 100)

with open('keyvars.json', 'w') as outfile:
    json.dump([last_update, current_hour, last_hour, last_day, rolling_7_days], outfile)
    print(json.dumps([last_update, current_hour, last_hour, last_day, rolling_7_days]))

