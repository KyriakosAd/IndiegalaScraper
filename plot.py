import pandas as pd
import matplotlib.pyplot as plt

try:
    df = pd.read_csv('dataframe.csv', index_col=0)
except FileNotFoundError:
    print("No dataframe.csv file found.")
    print("Exiting...")
    exit(1)

print(df)

df.plot(kind='scatter', x='Base Price', y='Final Price')
plt.show()
