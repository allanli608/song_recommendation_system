import pandas as pd
import os

directory = './csv_data/weather data toronto/'

csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]

dataframes = []
for file in csv_files:
    filepath = os.path.join(directory, file)
    df = pd.read_csv(filepath)
    dataframes.append(df)

merged_df = pd.concat(dataframes)

merged_df.sort_values(by='UTC_DATE', inplace=True)

# Save the merged dataframe to a new CSV file
merged_df['WEATHER_ENG_DESC'].ffill(inplace=True)
merged_df.to_csv('merged_weather_data.csv', index=False)
