"""
JNGTX Trading Dashboard - Data Extraction Script
Comparing JNGTX (Janus Henderson Tech) vs Fidelity Mutual Funds
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
import os

# Configuration - JNGTX vs Fidelity Across Categories
TICKERS = ['JNGTX', 'FSPTX', 'FXAIX', 'FCNTX', 'FBGRX']

# Fund descriptions for reference
FUND_INFO = {
    'JNGTX': 'Janus Henderson Global Technology',
    'FSPTX': 'Fidelity Select Technology (Tech Competitor)',
    'FXAIX': 'Fidelity 500 Index Fund (S&P 500 Benchmark)',
    'FCNTX': 'Fidelity Contrafund (Large-Cap Growth)',
    'FBGRX': 'Fidelity Blue Chip Growth (Blue Chip Focus)'
}

# Additional fund details
FUND_DETAILS = {
    'JNGTX': {'Category': 'Technology', 'Style': 'Active', 'Expense': '0.68%'},
    'FSPTX': {'Category': 'Technology', 'Style': 'Active', 'Expense': '0.68%'},
    'FXAIX': {'Category': 'Large Blend', 'Style': 'Index', 'Expense': '0.015%'},
    'FCNTX': {'Category': 'Large Growth', 'Style': 'Active', 'Expense': '0.82%'},
    'FBGRX': {'Category': 'Large Growth', 'Style': 'Active', 'Expense': '0.79%'}
}

DATA_FOLDER = 'data'
LOOKBACK_PERIOD = '3y'  # 3 years of historical data

# Create data folder if it doesn't exist
os.makedirs(DATA_FOLDER, exist_ok=True)

def fetch_ticker_data(ticker, period='3y'):
    """Fetch historical data for a single ticker"""
    try:
        details = FUND_DETAILS.get(ticker, {})
        print(f"Fetching {ticker}...", flush=True)
        print(f"   {FUND_INFO.get(ticker, 'Mutual Fund')}")
        print(f"   Category: {details.get('Category', 'N/A')} | "
              f"Style: {details.get('Style', 'N/A')} | "
              f"Expense Ratio: {details.get('Expense', 'N/A')}")
        
        stock = yf.Ticker(ticker)
        
        # Get historical data
        df = stock.history(period=period)
        
        if df.empty:
            print(f"     No data returned for {ticker}")
            return None
        
        # Reset index to make Date a column
        df.reset_index(inplace=True)
        
        # We only need these specific columns
        core_columns = {
            'Date': 'Date',
            'Open': 'Open',
            'High': 'High',
            'Low': 'Low',
            'Close': 'Close',
            'Volume': 'Volume'
        }
        
        # Select only columns that exist in the data
        selected_columns = []
        for col in core_columns.keys():
            if col in df.columns:
                selected_columns.append(col)
        
        # Create clean dataframe with only the columns we need
        clean_df = df[selected_columns].copy()
        
        # Add ticker and metadata columns
        clean_df['Ticker'] = ticker
        clean_df['Fund_Name'] = FUND_INFO.get(ticker, ticker)
        clean_df['Category'] = details.get('Category', 'Unknown')
        clean_df['Style'] = details.get('Style', 'Unknown')
        
        # Get current price
        current_price = clean_df['Close'].iloc[-1]
        
        print(f"   Current NAV: ${current_price:.2f} ({len(clean_df)} days of data)")
        
        return clean_df
        
    except Exception as e:
        print(f"    Error fetching {ticker}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def calculate_technical_indicators(df):
    """Calculate technical indicators for each ticker"""
    result_dfs = []
    
    for ticker in df['Ticker'].unique():
        ticker_df = df[df['Ticker'] == ticker].copy()
        ticker_df.sort_values('Date', inplace=True)
        
        # Moving Averages
        ticker_df['SMA_20'] = ticker_df['Close'].rolling(window=20).mean()
        ticker_df['SMA_50'] = ticker_df['Close'].rolling(window=50).mean()
        ticker_df['SMA_200'] = ticker_df['Close'].rolling(window=200).mean()
        
        # Daily Returns
        ticker_df['Daily_Return'] = ticker_df['Close'].pct_change() * 100
        
        # Volatility (20-day rolling standard deviation)
        ticker_df['Volatility_20'] = ticker_df['Daily_Return'].rolling(window=20).std()
        
        # Cumulative Return (from start of period)
        ticker_df['Cumulative_Return'] = ((ticker_df['Close'] / ticker_df['Close'].iloc[0]) - 1) * 100
        
        result_dfs.append(ticker_df)
    
    return pd.concat(result_dfs, ignore_index=True)

def main():
    """Main execution function"""
    print("=" * 70)
    print(" JNGTX vs Fidelity Mutual Funds - Performance Comparison")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nComparing JNGTX against {len(TICKERS)-1} Fidelity funds:")
    print()
    
    # Fetch data for all tickers
    all_data = []
    
    for i, ticker in enumerate(TICKERS, 1):
        print(f"[{i}/{len(TICKERS)}]")
        df = fetch_ticker_data(ticker, LOOKBACK_PERIOD)
        if df is not None:
            all_data.append(df)
        print()  # Blank line between tickers
    
    if not all_data:
        print("\n Failed to fetch any data. Check your internet connection.")
        return
    
    # Combine all dataframes
    print(" Combining data from all funds...")
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.sort_values(['Date', 'Ticker'], inplace=True)
    
    # Calculate technical indicators
    print("📈 Calculating technical indicators and performance metrics...")
    combined_df = calculate_technical_indicators(combined_df)
    
    # Save to CSV
    output_file = os.path.join(DATA_FOLDER, 'market_data.csv')
    combined_df.to_csv(output_file, index=False)
    
    print(f"\n Data saved to: {output_file}")
    print(f"   Total records: {len(combined_df):,}")
    print(f"   Columns: {len(combined_df.columns)}")
    
    # Format dates properly for display
    date_min = combined_df['Date'].min()
    date_max = combined_df['Date'].max()
    
    if isinstance(date_min, pd.Timestamp):
        print(f"   Date range: {date_min.strftime('%Y-%m-%d')} to {date_max.strftime('%Y-%m-%d')}")
    else:
        print(f"   Date range: {date_min} to {date_max}")
    
    # Also save latest prices only
    latest_df = combined_df.groupby('Ticker').tail(1)
    latest_file = os.path.join(DATA_FOLDER, 'market_data_latest.csv')
    latest_df.to_csv(latest_file, index=False)
    
    print(f"\n Latest fund data saved to: {latest_file}")
    print("\n" + "=" * 70)
    print(" PERFORMANCE SUMMARY (3-Year Period)")
    print("=" * 70)
    print(f"{'Ticker':<8} {'Fund Name':<35} {'NAV':<10} {'Return':<10} {'Vol':<8}")
    print("-" * 70)
    
    for _, row in latest_df.iterrows():
        fund_name = FUND_INFO.get(row['Ticker'], row['Ticker'])[:35]
        print(f"{row['Ticker']:<8} {fund_name:<35} "
              f"${row['Close']:>7.2f}  "
              f"{row['Cumulative_Return']:>+7.2f}%  "
              f"{row['Volatility_20']:>6.2f}%")
    
    # Find best performer
    best_return = latest_df.loc[latest_df['Cumulative_Return'].idxmax()]
    lowest_vol = latest_df.loc[latest_df['Volatility_20'].idxmin()]
    
    print("\n" + "=" * 70)
    print(" HIGHLIGHTS")
    print("=" * 70)
    print(f" Best Return:     {best_return['Ticker']} ({best_return['Cumulative_Return']:+.2f}%)")
    print(f"  Lowest Risk:     {lowest_vol['Ticker']} ({lowest_vol['Volatility_20']:.2f}% volatility)")
    
    # JNGTX specific stats
    jngtx_row = latest_df[latest_df['Ticker'] == 'JNGTX'].iloc[0]
    jngtx_rank = (latest_df['Cumulative_Return'] > jngtx_row['Cumulative_Return']).sum() + 1
    print(f" JNGTX Rank:      #{jngtx_rank} of {len(TICKERS)} funds")
    print(f" JNGTX Return:    {jngtx_row['Cumulative_Return']:+.2f}%")
    print(f" JNGTX Risk:      {jngtx_row['Volatility_20']:.2f}% volatility")
    
    print("\n" + "=" * 70)
    print(" Data extraction complete! Ready for Power BI dashboard!")
    print("=" * 70)

if __name__ == "__main__":
    main()