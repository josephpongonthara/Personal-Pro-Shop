import pandas as pd
import glob
import os

csv_files = glob.glob("/Users/josephpongonthara/Desktop/palantir_aip/personal_proshop_tennis/scraping_data/rackets_2_merge/*.csv")
df_list = [pd.read_csv(file) for file in csv_files]
combined_df = pd.concat(df_list, ignore_index=True)
combined_df.drop_duplicates(inplace=True)

combined_df.to_csv("tennis_rackets.csv", index=False)
print(f"Combined CSV saved to: {os.path.abspath('tennis_rackets.csv')}")
print("Columns merged:", combined_df.columns.tolist())