import json
import io
import re
import os
import csv
import pdfplumber
from datetime import datetime
from PyPDF2 import PdfReader
import boto3
from dotenv import load_dotenv
from flask import Flask, request, render_template, redirect, url_for
import logging

load_dotenv()

# Create a logger
logger = logging.getLogger('klm')

# Set the logging level to INFO
logger.setLevel(logging.INFO)

# Create a file handler for both INFO and ERROR messages
handler = logging.FileHandler('./logs/klm_' + datetime.now().strftime('%d-%m-%y') + '.log')

# Set the logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

DATE_PATTERN = r".*?(\d{2}/\d{2}/\d{2})"
GST_PATTERN = r"\d{2}[A-Za-z]{5}\d{4}[A-Za-z]{1}\d{1}[Z]{1}[A-Za-z\dc]{1}"
PNR_PATTERN = r"[A-Z0-9]{6}"
EMAIL_PATTERN = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
FLOAT_PATTERN = r"\d+\.\d+"

s3_client = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'), region_name=os.getenv('AWS_REGION'))

logger.info(f'Script Started')

app = Flask(__name__)

@app.route('/')
def upload_form():
    return render_template('upload_form.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        file_buffer = uploaded_file.read()
        parsed_data, table_data = klm_parser_helper(file_buffer, uploaded_file.filename)
        # Save result to CSV
        save_to_csv(parsed_data, table_data)  # Pass both parsed_data and table_data
        return redirect(url_for('upload_form'))
    return 'No file selected!'

def find_pattern(pattern, text, find_all=False):
    compiled_pattern = re.compile(pattern)
    if find_all:
        matches = compiled_pattern.findall(text)
    else:
        matches = compiled_pattern.search(text)
        if matches:
            matches = [matches.group(0)]
        else:
            matches = []
    return matches

def find_top_details(page_text):
    final_data = []
    print(page_text)
    document_type = None
    if "CREDIT NOTE" in page_text:
        document_type = "CREDIT NOTE"
    else:
        document_type = "TAX INVOICE"

    airline_name = "KLM Royal Dutch Airlines"

    airline_address = None
    original_invoice_date = None
    original_invoice_no = None
    invoice_no = None
    invoice_date = None
    credit_note_no = None
    credit_note_date = None
    hsn = None
    place_of_supply = None
    if document_type == "CREDIT NOTE":
        document_type = 'CR'
        airline_address = (page_text.split("CREDIT NOTE\n")[1].split("ORIGINAL")[0].replace("\n", " "))
        original_invoice_date = page_text.split("Corresponding Invoice Dt : ")[1].split("\n")[0]
        original_invoice_no = page_text.split("Corresponding Invoice No : ")[1].split("\n")[0]
        credit_note_date = page_text.split("Credit Note Dt : ")[1].split(
            "Service description"
        )[0]
        credit_note_no = page_text.split("Credit Note No : ")[1].split("\n")[0]
        place_of_supply_hsn_array = (
            # page_text.split("Place of supply : ")[1].split("\n")[0].split(" ")
            page_text.split("Place of supply : ")[1].split("(")[0].split(" ")
        )
        place_of_supply = (
            place_of_supply_hsn_array[0] + " " + place_of_supply_hsn_array[1]
        )

        if place_of_supply == "TAMILNADU":
            place_of_supply = "TAMIL NADU"

        hsn = place_of_supply_hsn_array[len(place_of_supply_hsn_array) - 1]

    elif document_type == "DEBIT NOTE":
        document_type = 'DB'
        airline_address = (
            page_text.split("DEBIT NOTE\n")[1].split("ORIGINAL")[0].replace("\n", " ")
        )
        original_invoice_date = page_text.split("Corresponding Invoice Dt : ")[1].split("\n")[0]
        original_invoice_no = page_text.split("Corresponding Invoice No : ")[1].split("\n")[0]
        credit_note_date = page_text.split("Debit Note Dt : ")[1].split(
            "Service description"
        )[0]
        credit_note_no = page_text.split("Debit Note No : ")[1].split("\n")[0]
        place_of_supply_hsn_array = (
            page_text.split("Place of supply : ")[1].split("\n")[0].split(" ")
        )
        place_of_supply = (
            place_of_supply_hsn_array[0] + " " + place_of_supply_hsn_array[1]
        )
        if place_of_supply == "TAMILNADU":
            place_of_supply = "TAMIL NADU"
        hsn = place_of_supply_hsn_array[len(place_of_supply_hsn_array) - 1]
    
    elif document_type == "TAX INVOICE":
        document_type = 'INV'
        airline_address = (
            page_text.split("Airlines\n")[1].split("ORIGINAL")[0].replace("\n", " ")
        )
        invoice_date = page_text.split("Date of issue ")[1].split("Invoice No")[0] if "Date of issue " in page_text else page_text.split("Invoice Date ")[1].split("Invoice No")[0]
        
        split_by_invoice1 = page_text.split("Invoice No : ")
        split_by_invoice2 = page_text.split("INVOICE No.")
        split_by_invoice = split_by_invoice1 if len(split_by_invoice1) > 1 else split_by_invoice2
        invoice_no = split_by_invoice[1].split("\n")[0]
        
        hsn_split = page_text.split("(HSN ")
        hsn = hsn_split[1].split(")")[0] if len(hsn_split) > 1 else None

        # place_of_supply1 = page_text.split("Place of supply ")
        # place_of_supply2 = page_text.split("Place of Supplier :")
        # place_of_supply = place_of_supply2 if(len(place_of_supply2)>1) else place_of_supply1
        # # place_of_supply = place_of_supply[1].split("Service description")[0]
        # place_of_supply = place_of_supply[1].split("(")[0]

        
        place_of_supply1 = page_text.split("Place of supply ")
        place_of_supply2 = page_text.split("Place of Supplier :")
        place_of_supply = place_of_supply2 if len(place_of_supply2) > 1 else place_of_supply1
        
             # Extract the place of supply
        place_of_supply = place_of_supply[1].split("(")[0].strip()

        # Normalize "TAMILNADU" to "TAMIL NADU"
        if place_of_supply.upper().replace(" ", "") == "TAMILNADU":
            place_of_supply = "TAMIL NADU"

    def find_gst_numbers(page_text):
        # Find all matches for the standard GST pattern
        all_gst_no = find_pattern(GST_PATTERN, page_text, find_all=True)
        
        # Custom extraction for cases where the pattern does not match
        if len(all_gst_no) < 2:
            # Example custom extraction logic for specific known cases
            potential_gst = page_text.split("GSTIN :")[1].split()[0]
            if potential_gst not in all_gst_no:
                all_gst_no.append(potential_gst)
        
        return all_gst_no
    
    all_gst_no = find_gst_numbers(page_text)

    print("all_gst------>", all_gst_no)
    airline_gst = all_gst_no[0]
    # customer_gst = page_text.split('GSTIN :')[1].split('\n')[0]
    customer_gst = all_gst_no[1]
    print("c_gst-------------->",customer_gst)
    customer_name = page_text.split(airline_gst + "\n")[1].split("Contact ")[0].split("\n")[0]
    email = find_pattern(EMAIL_PATTERN, page_text, find_all=True)[0]
    # customer_address_array = page_text.split("Contact details\n")[1].split("Accounting")[0]
    customer_address_array = page_text.split("Contact details\n")[1].split("Accounting " + email + "\n")
    
    
    customer_address = (customer_address_array[0] + " " + customer_address_array[1].split("\n")[0])
    print("---------------->",customer_address)
    if "GSTIN :" in customer_address:
        customer_address = customer_address.split("GSTIN")[0]
    elif "Credit Note" in customer_address:
        customer_address = customer_address.split("Credit Note")[0]
    else:
        customer_address = customer_address
    
    if customer_address == " ":
        customer_address = None
    print("---------------->",customer_address)
    ticket_no = page_text.split("Ticket Number ")[1].split("\n")[0][3:]
    try:
        pnr = page_text.split("PNR: ")[1].split("\n")[0]
    except:
        pnr = None
    try:
        passenger_name = page_text.split("Pax : ")[1].split("\n")[0]
    except:
        passenger_name = None
    total_journey = (
        page_text.split("Booking Class : ")[1].split("TICKET")[0].replace("\n", " ")
    )
    total_journey_array = total_journey.split(") (")
    origin = total_journey_array[0].split("-")[0].replace("(", "")
    
    try:
        destination = (
            total_journey_array[len(total_journey_array) - 1].split("-")[1].split(" /")[0]
        )
    except:
        destination = None
    # total_invoice_amount = page_text.split("TOTAL IN TICKET")[1].split("\n")[0].split(' ')[-2]
    total_invoice_amount = page_text.split("TOTAL IN TICKET CURRENCY")[1].split("\n")[0]

    final_data.extend(
        [
            {"key": "Airline Name", "value": airline_name},
            {"key": "Document_Type", "value": document_type},
            {"key": "Airline Address", "value": airline_address},
            {"key": "Airline GST Number", "value": airline_gst},
            {"key": "Airline Invoice Date", "value": invoice_date},
            {"key": "Airline Invoice Number", "value": invoice_no},
            {"key": "Original Invoice Date", "value": original_invoice_date},
            {"key": "Original Invoice Number", "value": original_invoice_no},
            {"key": "Ticket no", "value": ticket_no},
            {"key": "Customer Name", "value": customer_name},
            {"key": "Customer GST Number", "value": customer_gst},
            {"key": "Place of Supply", "value": place_of_supply},
            {"key": "Customer Address", "value": customer_address},
            {"key": "Credit Note No", "value": credit_note_no},
            {"key": "Credit Note Date", "value": credit_note_date},
            {"key": "HSN", "value": hsn},
            {"key": "PNR", "value": pnr},
            {"key": "Passenger Name", "value": passenger_name},
            {"key": "Origin", "value": origin},
            {"key": "Destination", "value": destination},
            {"key": "Total Invoice Amount", "value": total_invoice_amount},
        ]
    )
    return final_data

def find_table_details(table):
    print("table -----> ",table)
    taxable_value = None
    non_taxable_value = None
    igst_rate = None
    igst_amount = None
    cgst_rate = None
    cgst_amount = None
    sgst_rate = None
    sgst_amount = None
    total_gst = None

    if table[0]["3"]:
        taxable_value = table[1]["1"].split("\n ")[0]
        non_taxable_value = table[1]["1"].split("\n ")[1]
        igst_rate = find_pattern(FLOAT_PATTERN, table[0]["4"])[0]
        igst_amount = float(table[1]["4"].replace(",", ""))
        cgst_rate = find_pattern(FLOAT_PATTERN, table[0]["2"])[0]
        cgst_amount = float(table[1]["2"].replace(",", ""))

        sgst_rate = find_pattern(FLOAT_PATTERN, table[0]["3"])[0]
        sgst_amount = float(table[1]["3"].replace(",", ""))
        total_gst = cgst_amount + sgst_amount + igst_amount
    else:
        taxable_value = table[1]["1"].split("\n")[0]
        non_taxable_value = table[1]["1"].split("\n")[1]
        igst_rate = table[0]["4"].split("\n ")[1]
        igst_amount = float(table[1]["4"].split("\n")[0].replace(",", ""))
        all_rates = table[0]["2"].split("\n", 3)
        cgst_rate = all_rates[2]
        cgst_amount = float(table[1]["2"].split("\n")[0].replace(",", ""))
        sgst_rate = all_rates[3]
        sgst_amount = float(table[1]["2"].split("\n")[1].replace(",", ""))
        total_gst = cgst_amount + sgst_amount + igst_amount

    # Ensure all values are converted to string for consistency
    taxable_value = str(taxable_value)
    non_taxable_value = str(non_taxable_value)
    igst_rate = str(igst_rate)
    igst_amount = str(igst_amount)
    cgst_rate = str(cgst_rate)
    cgst_amount = str(cgst_amount)
    sgst_rate = str(sgst_rate)
    sgst_amount = str(sgst_amount)
    total_gst = str(total_gst)

    return [
        {"key": "Taxable Value", "value": taxable_value},
        {"key": "Non-Taxable Value", "value": non_taxable_value},
        {"key": "IGST Rate", "value": igst_rate},
        {"key": "IGST Amount", "value": igst_amount},
        {"key": "CGST Rate", "value": cgst_rate},
        {"key": "CGST Amount", "value": cgst_amount},
        {"key": "SGST Rate", "value": sgst_rate},
        {"key": "SGST Amount", "value": sgst_amount},
        {"key": "Total GST", "value": total_gst},
    ]

def klm_parser_helper(file_buffer, filename):
    parsed_data = []
    table_data = []  # Initialize an empty list for table data
    with pdfplumber.open(io.BytesIO(file_buffer)) as pdf:
        for i in range(len(pdf.pages)):
            page = pdf.pages[i]
            page_text = page.extract_text()
            try:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        for column in row:
                            table_data.append(column)
                    print(find_table_details(table))
            except Exception as e:
                logger.error(f"Error extracting tables: {str(e)}")

            top_details = find_top_details(page_text)
            parsed_data.extend(top_details)
    return parsed_data, table_data  # Return both parsed data and table data


def save_to_csv(parsed_data, table_data):
    # Get the current date for the CSV file name
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Define the path for the CSV file
    csv_file_path = f"./output/klm_parsed_data_{current_date}.csv"

    # Write the data to the CSV file
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)

        # Write top details
        for i in range(0, len(parsed_data), 2):
            row = [parsed_data[i]["key"], parsed_data[i]["value"]]
            if i + 1 < len(parsed_data):
                row.extend([parsed_data[i + 1]["key"], parsed_data[i + 1]["value"]])
            writer.writerow(row)

        # Write table details
        for row in table_data:
            writer.writerow(row)

    return csv_file_path






app.run(debug=True)