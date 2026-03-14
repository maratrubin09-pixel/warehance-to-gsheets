"""
Brand configuration — single source of truth for colors, emails, column widths.
Used by sheets_writer.py, create_all_sheets.py, client_discovery.py.
"""

# Brand colors (RGB 0-1 for Google Sheets API)
DEEP_PURPLE = {"red": 0.176, "green": 0.106, "blue": 0.412}
PURPLE = {"red": 0.424, "green": 0.247, "blue": 0.710}
PINK = {"red": 0.914, "green": 0.118, "blue": 0.549}
GREEN = {"red": 0.0, "green": 0.769, "blue": 0.549}
LIGHT_PURPLE = {"red": 0.941, "green": 0.902, "blue": 1.0}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}

# Sharing settings
OWNER_EMAIL = "office@fastprepusa.com"
EDITOR_EMAIL = "bwmodnick@gmail.com"

# AllReports column widths (pixels)
COL_WIDTHS_ALLREPORTS = [80, 160, 240, 110, 150, 110, 110, 110]
# Columns: Date | Order Number | Tracking | Pick&Pack fee | Packaging Type | Packaging Cost | Shipping Cost | Total

# Payments column widths (pixels)
COL_WIDTHS_PAYMENTS = [120, 120, 120, 140, 180, 180]

# Default billing profile ID (Standard — used for all clients except 001 and 154)
DEFAULT_BILLING_PROFILE_ID = 231185181454

# Special billing profiles
SPECIAL_BILLING_PROFILES = {
    # client_number: billing_profile_id
    "001": 231185182410,   # Sarmali — custom profile
    "154": 231185181547,   # Liza Herman — 10000+ profile
}
