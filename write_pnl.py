"""Write daily P&L data to the P&L Dashboard spreadsheet."""
import gspread
from google.oauth2.service_account import Credentials

PNL_SPREADSHEET_ID = "1lQz6_Vx4rx0j1WwBhgJRVF25IC6DT3AqCGBBxLi6C1Y"

def write_pnl_row(service_account_file, client_number, client_name, date_str, transform_result):
    """
    Write one row per client per day to P&L Data tab.
    
    transform_result is the dict from transform_bill_details() containing:
      - report_rows, payments_row, grand_total, etc.
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    gc = gspread.authorize(creds)
    
    ss = gc.open_by_key(PNL_SPREADSHEET_ID)
    ws = ss.worksheet("Data")
    
    # Aggregate from report_rows
    report_rows = transform_result["report_rows"]
    
    storage = 0
    return_processing = 0
    return_labels = 0
    pick_pack = 0
    packaging_rev = 0
    shipping_rev = 0
    orders = 0
    
    for r in report_rows:
        onum = r.get("Order Number", "")
        if onum == "Storage":
            storage = r["Total"]
        elif onum == "Return Processing Charges":
            return_processing = r["Total"]
        elif onum == "Return Labels Charges":
            return_labels = r["Total"]
        elif onum == "Total":
            continue
        else:
            # Regular order
            orders += 1
            s = r.get("Shipping cost", 0) or 0
            p = r.get("Pick&Pack fee", 0) or 0
            pkg = r.get("Package cost", 0) or 0
            shipping_rev += float(s)
            pick_pack += float(p)
            packaging_rev += float(pkg)
    
    total_revenue = storage + return_processing + pick_pack + packaging_rev + shipping_rev + return_labels
    # Shipping Cost and Packaging Cost (carrier/material) - TBD, 0 for now
    shipping_cost = 0
    packaging_cost = 0
    total_cost = shipping_cost + packaging_cost
    gross_profit = total_revenue - total_cost
    margin = round(gross_profit / total_revenue * 100, 1) if total_revenue > 0 else 0
    
    row = [
        date_str,
        "'" + str(client_number),
        client_name,
        orders,
        round(storage, 2),
        round(return_processing, 2),
        round(pick_pack, 2),
        round(packaging_rev, 2),
        round(shipping_rev, 2),
        round(return_labels, 2),
        round(shipping_cost, 2),
        round(packaging_cost, 2),
        round(total_revenue, 2),
        round(total_cost, 2),
        round(gross_profit, 2),
        margin,
    ]
    
    ws.append_row(row, value_input_option="USER_ENTERED")
    return row


if __name__ == "__main__":
    print("Use write_pnl_row() from agent.py")
