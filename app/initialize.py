import pulse
import pandas as pd
result = pulse.collect_data(end_date = '2017-08-20')
result = pulse.scale_data(result)
result.to_pickle('result.pd')
result.to_csv('result.csv')
pulse.group_by_day(result).to_csv('result-grouped.csv')