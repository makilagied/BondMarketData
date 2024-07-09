from flask import Flask, request, render_template, redirect, url_for, flash, send_file
import pandas as pd
from bs4 import BeautifulSoup
import re
import io
import psycopg2

app = Flask(__name__)
app.secret_key = "supersecretkey"

def process_html_content(content):
    # Parse the HTML content
    soup = BeautifulSoup(content, 'html.parser')

    # Extract text from the HTML
    text = soup.get_text(separator=" ")

    # Find the index of the first occurrence of "Bond No."
    start_index = text.find("Bond No.")

    if start_index == -1:
        return None, "Pattern not found in the text."
    else:
        # Extract the text after the first occurrence of "Bond No."
        relevant_text = text[start_index:]

        # Clean text: remove all unwanted spaces, tabs, and newlines
        cleaned_text = re.sub(r'\s+', '', relevant_text)

        # Define the regex pattern to match the data entries with flexible separators
        pattern = re.compile(
            r'(\w{3})\.?(\d{1,2})\.?(\d{1,2}\.\d{2})\.?(\d{2}/\d{2}/\d{4})\.?(\d{2}/\d{2}/\d{4})\.?(\d{1})\.?(\d{2}/\d{2}/\d{4})\.?(\d+\.\d{5})\.?(\d+\.\d{4})\.?(\d+\.\d{4})'
        )

        # Process the cleaned text to extract structured data
        data = []
        for match in re.finditer(pattern, cleaned_text):
            data.append(match.groups())

        if not data:
            return None, "No matching data found."
        else:
            # Convert to DataFrame
            columns = ["Bond_No.", "Term (Years)", "Coupon (%)", "Issue Date", "Maturity Date", "Deals", "Trade Date", "Amount (Bln TZS)", "Price (%)", "Yield"]
            df = pd.DataFrame(data, columns=columns)

            # Return DataFrame and success message
            return df, None

def insert_data_into_db(df):
    # Connect to PostgreSQL Database
    conn = psycopg2.connect(
        dbname="",
        user="postgres",
        password="",
        host="",
        port="5432"
    )
    cur = conn.cursor()

    # Check if any trade dates already exist in the table
    trade_dates = tuple(df['Trade Date'].unique())
    query = f'SELECT "TradeDate" FROM bond_data WHERE "TradeDate" IN %s'
    cur.execute(query, (trade_dates,))
    existing_trade_dates = {row[0] for row in cur.fetchall()}

    if existing_trade_dates:
        print(f"Trade dates {existing_trade_dates} already exist. Skipping insertion.")
    else:
        # Insert data into PostgreSQL table
        query = '''INSERT INTO bond_data ("Bond_No.", "Term", "Coupon", "IssueDate", "MaturityDate", "Deals", "TradeDate", "Amount", "Price", "Yield") 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = [(
            row['Bond_No.'], row['Term (Years)'], row['Coupon (%)'], row['Issue Date'], row['Maturity Date'], 
            row['Deals'], row['Trade Date'], row['Amount (Bln TZS)'], row['Price (%)'], row['Yield']
        ) for _, row in df.iterrows()]
        cur.executemany(query, values)
        print("Data has been successfully inserted.")

    # Commit changes
    conn.commit()

    # Close database connection
    cur.close()
    conn.close()

@app.route('/')
def upload_form():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash("No file part")
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        flash("No selected file")
        return redirect(request.url)
    if file:
        content = file.read().decode('utf-8')  # Ensure the content is decoded to a string
        df, error = process_html_content(content)
        if error:
            flash(error)
            return redirect(request.url)
        else:
            # Insert data into PostgreSQL database
            insert_data_into_db(df)
            
            # Prepare and send Excel file
            output = io.BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)
            return send_file(output, download_name="DSE_bond_data.xlsx", as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
