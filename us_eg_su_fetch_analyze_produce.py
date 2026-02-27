# us_eg_su_fetch_analyze_produce.py
import logging
import os
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
import plotly.express as px
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ─── Configuration ────────────────────────────────────────────────
TICKERS = [
    "AAPL", "TSLA", "VOD.L", "BP.L", "SAP.DE",
    "COMI.CA", "HRHO.CA", "2222.SR", "1120.SR",
    "005930.KS", "RELIANCE.NS"
]

DB_URL = "postgresql+psycopg2://postgres:karim@postgres:5432/stocks_2026"
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "stock-reports"
OUTPUT_DIR = "/tmp/outputs"  # safe writable location
EMAIL_FROM = "90.karim@gmail.com"
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
# EMAIL_PASSWORD: ${EMAIL_PASSWORD}
EMAIL_PASSWORD="rakp ehju ldjy zazr"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─── Database ─────────────────────────────────────────────────────
engine = create_engine(DB_URL)

# ─── Data Fetch ───────────────────────────────────────────────────
def fetch_data(tickers):
    dfs = []
    for t in tickers:
        try:
            # progress=False to reduce spam, auto_adjust=True to get clean OHLC
            raw = yf.download(t, period="1mo", interval="1d", progress=False, auto_adjust=True, repair=True)
            
            if raw.empty:
                logging.warning(f"yfinance returned empty DataFrame for {t}")
                continue

            # Handle possible multi-index (very common now)
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)  # flatten to single level

            df = raw.reset_index()

            # Normalize date column name (yfinance sometimes uses 'Date', sometimes not)
            date_col = [c for c in df.columns if 'date' in c.lower()]
            if date_col:
                df.rename(columns={date_col[0]: 'Datetime'}, inplace=True)
            elif 'index' in df.columns:
                df.rename(columns={'index': 'Datetime'}, inplace=True)
            else:
                logging.warning(f"No date column found for {t} – skipping")
                continue

            # Force-add Ticker
            df['Ticker'] = t

            # Core columns we need – use .get() to avoid KeyError
            core_cols = ['Datetime', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
            available_cols = [c for c in core_cols if c in df.columns]
            if len(available_cols) < 5:  # at least Datetime + Ticker + OHLC
                missing = set(core_cols) - set(available_cols)
                logging.warning(f"Missing critical columns for {t}: {missing} – skipping")
                continue

            df = df[available_cols]

            # Clean NaN in price/volume
            df = df.dropna(subset=['Open', 'High', 'Low', 'Close'], how='all')
            if df.empty:
                logging.warning(f"No valid rows left after cleaning for {t}")
                continue

            dfs.append(df)
            logging.info(f"Successfully fetched & cleaned {len(df)} rows for {t}")

        except Exception as e:
            logging.error(f"Exception while fetching {t}: {str(e)}")

    if not dfs:
        logging.error("No ticker returned usable data")
        return pd.DataFrame()

    final_df = pd.concat(dfs, ignore_index=True)
    final_df = final_df.sort_values(['Ticker', 'Datetime'])
    logging.info(f"Final combined DataFrame: {len(final_df)} rows, {final_df['Ticker'].nunique()} tickers")
    return final_df

# ─── Save to DB ───────────────────────────────────────────────────
def save_to_db(df):
    if df.empty:
        logging.warning("No data to save – skipping DB write")
        return

    required = ['Ticker', 'Datetime']
    missing = [c for c in required if c not in df.columns]
    if missing:
        logging.error(f"Cannot save – missing columns: {missing}")
        return

    try:
        df_clean = df.drop_duplicates(subset=['Ticker', 'Datetime'], keep='last')
        df_clean.to_sql('stock_history', engine, if_exists='append', index=False)
        logging.info(f"Saved {len(df_clean)} rows to database")
    except Exception as e:
        logging.error(f"DB save failed: {e}")

# ─── Analysis & Visualization ─────────────────────────────────────
def analyze_and_visualize(df):
    """
    Generates 24 plots:
    - 11 Close price line charts (one per ticker)
    - 11 Volume bar charts (one per ticker)
    - 1 global Mean/Min/Max bar chart
    - 1 combined Close prices line chart
    Returns list of 24 file paths + summary text
    """
    if df.empty:
        logging.warning("No data to analyze")
        return [], "No data available"

    paths = []
    summary_lines = [f"Processed {len(df)} rows across {df['Ticker'].nunique()} tickers.\n"]

    # Make sure Datetime is datetime type
    df['Datetime'] = pd.to_datetime(df['Datetime'])

    # ── Per-ticker plots (22 plots) ─────────────────────────────────
    for ticker in sorted(df['Ticker'].unique()):
        df_t = df[df['Ticker'] == ticker].sort_values('Datetime')

        if df_t.empty:
            continue

        # 1. Close price line
        fig_close = px.line(
            df_t,
            x='Datetime',
            y='Close',
            title=f"{ticker} - Close Price (Last Month)",
            labels={'Close': 'Price (USD)', 'Datetime': 'Date'}
        )
        fig_close.update_layout(showlegend=False, xaxis_title="Date", yaxis_title="Close Price")
        path_close = os.path.join(OUTPUT_DIR, f"{ticker}_close_{datetime.now().strftime('%Y%m%d')}.png")
        fig_close.write_image(path_close, scale=1.6, width=1000, height=500)
        paths.append(path_close)

        # 2. Volume bar
        fig_vol = px.bar(
            df_t,
            x='Datetime',
            y='Volume',
            title=f"{ticker} - Trading Volume",
            labels={'Volume': 'Volume', 'Datetime': 'Date'}
        )
        fig_vol.update_layout(xaxis_title="Date", yaxis_title="Volume")
        path_vol = os.path.join(OUTPUT_DIR, f"{ticker}_volume_{datetime.now().strftime('%Y%m%d')}.png")
        fig_vol.write_image(path_vol, scale=1.6, width=1000, height=500)
        paths.append(path_vol)

        # Summary line for this ticker
        mean_close = df_t['Close'].mean()
        summary_lines.append(f"{ticker}: Mean Close = {mean_close:.2f} | {len(df_t)} days")

    # ── Global summary plots (2 more) ────────────────────────────────
    # 23. All tickers - Mean/Min/Max bar
    stats = df.groupby('Ticker')['Close'].agg(['mean', 'min', 'max']).reset_index()
    stats.columns = ['Ticker', 'Mean', 'Min', 'Max']

    fig_global_stats = px.bar(
        stats,
        x='Ticker',
        y=['Mean', 'Min', 'Max'],
        barmode='group',
        title="All Stocks - Price Statistics (Mean / Min / Max)",
        labels={'value': 'Price (USD)', 'variable': 'Statistic'}
    )
    fig_global_stats.update_layout(xaxis_title="Ticker", yaxis_title="Price (USD)")
    global_stats_path = os.path.join(OUTPUT_DIR, f"global_stats_{datetime.now().strftime('%Y%m%d')}.png")
    fig_global_stats.write_image(global_stats_path, scale=1.6, width=1200, height=600)
    paths.append(global_stats_path)

    # 24. Combined close prices (all tickers in one chart)
    fig_combined = px.line(
        df,
        x='Datetime',
        y='Close',
        color='Ticker',
        title="All Tickers - Close Price Comparison",
        labels={'Close': 'Close Price (USD)', 'Datetime': 'Date'}
    )
    fig_combined.update_layout(xaxis_title="Date", yaxis_title="Close Price")
    combined_path = os.path.join(OUTPUT_DIR, f"combined_close_{datetime.now().strftime('%Y%m%d')}.png")
    fig_combined.write_image(combined_path, scale=1.6, width=1200, height=600)
    paths.append(combined_path)

    # Final summary text
    summary = "\n".join(summary_lines) + f"\n\nTotal charts attached: {len(paths)}"
    logging.info(summary)

    return paths, summary
# ─── Email ────────────────────────────────────────────────────────
def send_email(attachments, body):
    if not EMAIL_PASSWORD:
        logging.warning("EMAIL_PASSWORD not set → skipping email")
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_FROM
    msg['Subject'] = f"Stock Report {datetime.now().strftime('%Y-%m-%d')}"
    msg.attach(MIMEText(body, 'plain'))

    for path in attachments:
        if os.path.exists(path):
            with open(path, 'rb') as f:
                img = MIMEImage(f.read())
                img.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
                msg.attach(img)

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"Email failed: {e}")

# ─── Kafka Producer ───────────────────────────────────────────────
def push_to_kafka(summary, attachments):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        payload = {
            'timestamp': datetime.now().isoformat(),
            'summary': summary,
            'attachments': [os.path.basename(p) for p in attachments] if attachments else [],
            'status': 'success'
        }
        producer.send(TOPIC, value=payload)
        producer.flush()
        logging.info("Message sent to Kafka")
    except KafkaError as e:
        logging.error(f"Kafka produce failed: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

# ─── Main ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tickers', default=','.join(TICKERS))
    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(',')]

    logging.info("Starting stock pipeline...")
    df = fetch_data(tickers)

    if df.empty:
        logging.error("No data fetched. Exiting.")
        exit(1)

    save_to_db(df)
    paths, summary = analyze_and_visualize(df)
    send_email(paths, summary)
    push_to_kafka(summary, paths)

    logging.info("Pipeline finished.")