import pandas as pd
import numpy as np

data = []
countries   = ["USA", "MEX", "CAN", "DEU", "ITA", "FRA", "CHN", "RUS", "SAU", "ARE", "GBR", "TUR", "IND", "BRA"]
candy_types = ["chocolate bar", "white chocolate bar", "dark chocolate bar", "blueberry bubblegum", "caramel popcorn", "peanut butter pops", "chocolate cookies", "butter cookies", "gummy bears", "lollipops"]

for i in range(100000):
  country = np.random.choice(countries)
  candy   = np.random.choice(candy_types)
  sales   = np.random.randint(100000, 10000000)

  data.append({'country': country, 'candy': candy, 'sales': sales})

df = pd.DataFrame(data)
df.to_csv('candy_sales.csv', index=False) # We don't need to store the index