import pandas as pd
import polars as pl
#from fetch_utils import fetch_league
#from analytic_utils import get_lineup, get_best_lineup, get_total_tds, get_best_trio, get_lineup_efficiency, get_score_surprise, get_idiot_score

final_df = pl.read_csv('Data/Projections/Pinnacle_Props.csv')
#print(final_df)

prop_df = final_df
            #
            
#prop_path = 'Data/Projections/Pinnacle_Props.csv'
#prop_df.write_csv(prop_path)
print(prop_df.head())

