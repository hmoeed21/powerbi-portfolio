import yfinance as yf
import pandas as pd

print("Testing data download...")

# Test JNGTX only first
ticker = yf.Ticker("JNGTX")
df = ticker.history(period="5d")

print("\nColumns returned:")
print(df.columns.tolist())

print("\nFirst few rows:")
print(df.head())

print("\nData shape:", df.shape)
print("Success!")