import pandas as pd
import os

def create_string_id_column(csv_path):
    df = pd.read_csv(csv_path)

    required_cols = ['String', 'Ref. Ten. (lbs)', 'Swing Speed']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: '{col}' in the dataset.")

    df.insert(0, 'string_id', df['String'].astype(str) + ' - ' +
                         df['Ref. Ten. (lbs)'].astype(str) + ' - ' +
                         df['Swing Speed'].astype(str))

    df = df.drop_duplicates(subset='string_id')
    base, ext = os.path.splitext(csv_path)
    new_path = f"{base}_with_ids{ext}"
    df.to_csv(new_path, index=False)
    print(f"File with 'string_id' created @: {os.path.abspath(new_path)}")

create_string_id_column('/Users/josephpongonthara/Desktop/palantir_aip/personal_proshop_tennis/scraping_data/tennis_strings_v1.csv')
