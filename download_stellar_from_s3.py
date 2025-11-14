#!/usr/bin/env python3
"""
Stellar Business Data Processing Module

Downloads gzipped DATA files from S3, parses CSVs, and inserts into Oracle staging tables.
Processes all 29 Stellar tables including:
- Core reference data (9 tables): customers, locations, seasons, accessories, amenities, categories, holidays
- Booking system (4 tables): bookings, booking_boats, booking_payments, booking_accessories
- Boat inventory & pricing (11 tables): styles, style_boats, style_groups, style_times, style_prices, etc.
- Point of sale (5 tables): pos_items, pos_sales, fuel_sales, closed_dates, blacklists
"""

import boto3
import logging
import csv
import io
import sys
from stellar_db_functions import OracleConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('stellar_processing.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)


# =============================================================================
# DEPRECATED CSV PARSING FUNCTIONS
# =============================================================================
# NOTE: The functions below (parse_customers_data, parse_locations_data, etc.)
# are NO LONGER USED. They were designed for CSV files in S3 subdirectories.
# 
# The new approach (as of Nov 2025) parses SQL dump files directly using
# extract_table_data_from_sql() function. The SQL dumps are downloaded as
# prod_resilient_YYYY-MM-DD_HH_MM-DATA.sql.gz files from the bucket root.
#
# These CSV parsing functions are kept for reference only and may be removed
# in the future.
# =============================================================================

def parse_int(value):
    """Convert string to int or None if empty."""
    if value == '' or value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_float(value):
    """Convert string to float or None if empty."""
    if value == '' or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_date(value):
    """Convert date/timestamp string to None if empty (for Oracle TO_DATE/TO_TIMESTAMP)."""
    if value == '' or value is None:
        return None
    return value


def parse_customers_data(csv_content):
    """Parse customers CSV - 52 columns matching actual CSV structure."""
    from datetime import datetime
    
    def convert_date(value):
        """Convert date string to datetime object or None."""
        if not value or value == '':
            return None
        try:
            return datetime.strptime(value, '%Y-%m-%d')
        except:
            return None
    
    def convert_timestamp(value):
        """Convert timestamp string to datetime object or None."""
        if not value or value == '':
            return None
        try:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except:
            return None
    
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('user_id')),
            parse_int(row.get('club_principal_user_id')),
            parse_int(row.get('coupon_id')),
            parse_int(row.get('club_tier_id')),
            row.get('firstname'),
            row.get('lastname'),
            row.get('middlename'),
            row.get('gender'),
            row.get('phone'),
            row.get('cell'),
            row.get('emergencyname'),
            row.get('emergencyphone'),
            row.get('secondary_email'),
            row.get('billing_street1'),
            row.get('billing_street2'),
            row.get('billing_city'),
            row.get('billing_state'),
            row.get('billing_country'),
            row.get('billing_zip'),
            row.get('mailing_street1'),
            row.get('mailing_street2'),
            row.get('mailing_city'),
            row.get('mailing_state'),
            row.get('mailing_country'),
            row.get('mailing_zip'),
            parse_int(row.get('numkids')),
            row.get('referrer'),
            row.get('services'),
            row.get('dob'),  # Keep as string - table expects NVARCHAR2
            row.get('dlstate'),
            row.get('dlcountry'),
            row.get('dlnumber'),
            row.get('notes'),
            row.get('internal_notes'),
            row.get('club_status'),
            convert_date(row.get('club_start_date')),
            parse_int(row.get('club_use_recurring_billing')),
            convert_date(row.get('club_recurring_billing_start_date')),
            parse_float(row.get('balance')),
            row.get('bdrc'),
            parse_int(row.get('penalty_points')),
            parse_float(row.get('open_balance_threshold')),
            convert_date(row.get('club_end_date')),
            row.get('cc_saved_name'),
            row.get('cc_saved_last4'),
            convert_date(row.get('cc_saved_expiry')),
            row.get('cc_saved_profile_id'),
            row.get('cc_saved_method_id'),
            row.get('cc_saved_address_id'),
            row.get('external_id'),
            convert_timestamp(row.get('created_at')),
            convert_timestamp(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} customer records")
    return data_rows


def parse_locations_data(csv_content):
    """Parse locations CSV - 22 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            row.get('code'),
            row.get('location_name'),
            row.get('location_type'),
            parse_int(row.get('minimum_1')),
            parse_int(row.get('minimum_2')),
            parse_int(row.get('delivery')),
            parse_int(row.get('frontend')),
            row.get('pricing'),
            parse_int(row.get('is_internal')),
            parse_int(row.get('is_canceled')),
            row.get('cancel_reason'),
            parse_date(row.get('cancel_date')),
            parse_int(row.get('is_transferred')),
            row.get('transfer_destination'),
            row.get('module_type'),
            row.get('operating_location'),
            row.get('zoho_id'),
            row.get('zcrm_id'),
            parse_int(row.get('is_active')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} location records")
    return data_rows


def parse_seasons_data(csv_content):
    """Parse seasons CSV - 20 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('season_name'),
            parse_date(row.get('season_start')),
            parse_date(row.get('season_end')),
            row.get('status'),
            row.get('weekday_min_start_time'),
            row.get('weekday_max_start_time'),
            row.get('weekday_min_end_time'),
            row.get('weekday_max_end_time'),
            row.get('weekend_min_start_time'),
            row.get('weekend_max_start_time'),
            row.get('weekend_min_end_time'),
            row.get('weekend_max_end_time'),
            row.get('holiday_min_start_time'),
            row.get('holiday_max_start_time'),
            row.get('holiday_min_end_time'),
            row.get('holiday_max_end_time'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} season records")
    return data_rows


def parse_accessories_data(csv_content):
    """Parse accessories CSV - 19 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    def convert_yes_no(value):
        """Convert boolean-like value to Yes/No string."""
        if not value or value.strip() == '' or value == '0':
            return 'No'
        return 'Yes' if value == '1' else 'No'
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('accessory_name'),
            parse_int(row.get('position')),
            parse_int(row.get('frontend_position')),
            row.get('short_name'),
            row.get('abbreviation'),
            row.get('image'),
            parse_float(row.get('price')),
            parse_float(row.get('deposit_amount')),
            convert_yes_no(row.get('tax_exempt')),
            parse_int(row.get('max_overlapping_rentals')),
            parse_int(row.get('frontend_qty_limit')),
            convert_yes_no(row.get('use_striped_background')),
            parse_int(row.get('backend_available_days')),
            parse_int(row.get('frontend_available_days')),
            parse_int(row.get('max_same_departures')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} accessory records")
    return data_rows


def parse_accessory_options_data(csv_content):
    """Parse accessory_options CSV - 6 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    def convert_yes_no(value):
        """Convert boolean-like value to Yes/No string."""
        if not value or value.strip() == '' or value == '0':
            return 'No'
        return 'Yes' if value == '1' else 'No'
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('accessory_id')),
            row.get('value'),
            convert_yes_no(row.get('use_striped_background')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} accessory option records")
    return data_rows


def parse_accessory_tiers_data(csv_content):
    """Parse accessory_tiers CSV - 8 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        # Default min_hours and max_hours to 0 if NULL
        min_hours = parse_int(row.get('min_hours'))
        max_hours = parse_int(row.get('max_hours'))
        
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('accessory_id')),
            min_hours if min_hours is not None else 0,
            max_hours if max_hours is not None else 0,
            parse_float(row.get('price')),
            parse_int(row.get('accessory_option_id')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} accessory tier records")
    return data_rows


def parse_amenities_data(csv_content):
    """Parse amenities CSV - 16 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    def convert_yes_no(value):
        """Convert boolean-like value to Yes/No string."""
        if not value or value.strip() == '' or value == '0':
            return 'No'
        return 'Yes' if value == '1' else 'No'
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('amenity_name'),
            convert_yes_no(row.get('frontend_display')),
            row.get('frontend_name'),
            parse_int(row.get('frontend_position')),
            convert_yes_no(row.get('featured')),
            convert_yes_no(row.get('filterable')),
            row.get('icon'),
            row.get('type'),
            row.get('options'),
            row.get('prefix'),
            row.get('suffix'),
            row.get('description'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} amenity records")
    return data_rows


def parse_categories_data(csv_content):
    """Parse categories CSV - 15 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    def convert_yes_no(value):
        """Convert boolean-like value to Yes/No string."""
        if not value or value.strip() == '' or value == '0':
            return 'No'
        return 'Yes' if value == '1' else 'No'
    
    for row in reader:
        # filter_unit_type_position is a string, default to 'First' if NULL
        filter_position = row.get('filter_unit_type_position')
        if not filter_position or filter_position.strip() == '':
            filter_position = 'First'
        
        # min_nights_multi_day defaults to 1 if NULL
        min_nights = parse_int(row.get('min_nights_multi_day'))
        if min_nights is None:
            min_nights = 1
        
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('category_name'),
            convert_yes_no(row.get('frontend_display')),
            row.get('frontend_name'),
            row.get('frontend_type'),
            parse_int(row.get('frontend_position')),
            convert_yes_no(row.get('filter_unit_type_enabled')),
            row.get('filter_unit_type_name'),
            filter_position,
            min_nights,
            row.get('calendar_banner_text'),
            row.get('description'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} category records")
    return data_rows


def parse_holidays_data(csv_content):
    """Parse holidays CSV - 2 columns (no ID column)."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('location_id')),
            parse_date(row.get('holiday_date'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} holiday records")
    return data_rows


def parse_bookings_data(csv_content):
    """Parse bookings CSV - 82 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            parse_int(row.get('customer_id')),
            parse_int(row.get('creator_id')),
            parse_int(row.get('admin_id')),
            row.get('billing_firstname'),
            row.get('billing_lastname'),
            row.get('billing_street1'),
            row.get('billing_street2'),
            row.get('billing_city'),
            row.get('billing_state'),
            row.get('billing_country'),
            row.get('billing_zip'),
            row.get('cc_saved_name'),
            parse_int(row.get('cc_saved_last4')),
            row.get('cc_saved_profile_id'),
            row.get('cc_saved_method_id'),
            row.get('cc_saved_address_id'),
            row.get('cc_preauth_id'),
            parse_float(row.get('cc_preauth_amount')),
            row.get('cc_connect_type'),
            row.get('cc_connect_id'),
            parse_float(row.get('accessories_custom_price')),
            parse_float(row.get('accessories_total')),
            parse_float(row.get('insurance')),
            parse_float(row.get('pets')),
            parse_float(row.get('parking')),
            parse_int(row.get('parking_override')),
            parse_float(row.get('boats_total')),
            parse_float(row.get('pos_total')),
            parse_int(row.get('use_club_credits')),
            parse_int(row.get('no_show_fee')),
            parse_int(row.get('cancellation_fee')),
            parse_float(row.get('club_fees')),
            parse_int(row.get('club_fees_override')),
            parse_float(row.get('subtotal')),
            parse_float(row.get('convenience_fee')),
            parse_int(row.get('convenience_fee_waived')),
            parse_float(row.get('internal_application_fee')),
            parse_float(row.get('tax_1')),
            parse_int(row.get('tax_1_exempt')),
            parse_float(row.get('tax_1_rate_override')),
            parse_float(row.get('tax_2')),
            parse_int(row.get('tax_2_exempt')),
            parse_float(row.get('checkin_tax_1')),
            parse_float(row.get('checkin_tax_2')),
            parse_float(row.get('checkin_total')),
            parse_float(row.get('deposit_total')),
            parse_int(row.get('deposit_override')),
            parse_int(row.get('deposit_waived')),
            parse_float(row.get('gratuity')),
            parse_float(row.get('grand_total')),
            parse_float(row.get('adjustment_total')),
            parse_float(row.get('amount_paid')),
            row.get('notes'),
            row.get('notes_contract'),
            row.get('notes_from_customer'),
            row.get('notes_from_customer_contract'),
            row.get('notes_for_customer'),
            row.get('notes_for_customer_contract'),
            row.get('frontend'),
            row.get('is_on_hold'),
            row.get('is_locked'),
            row.get('is_finalized'),
            row.get('is_canceled'),
            row.get('override_turnaround_time'),
            row.get('cancellation_type'),
            row.get('bypass_club_restrictions'),
            row.get('renters_insurance_interest'),
            parse_int(row.get('coupon_id')),
            row.get('coupon_type'),
            parse_float(row.get('coupon_amount')),
            parse_float(row.get('discount_total')),
            parse_int(row.get('agent_id')),
            row.get('agent_name'),
            row.get('referrer_id'),
            parse_int(row.get('safety_reminder')),
            parse_int(row.get('deleted_admin_id')),
            parse_date(parse_date(row.get('created_at'))),
            parse_date(parse_date(row.get('updated_at'))),
            parse_date(row.get('finalized_at')),
            parse_date(parse_date(row.get('deleted_at')))
        ))
    
    logger.info(f"Parsed {len(data_rows)} booking records")
    return data_rows


def parse_booking_boats_data(csv_content):
    """Parse booking_boats CSV - 57 columns matching actual CSV structure."""
    from datetime import datetime
    
    def convert_timestamp(value):
        """Convert timestamp string to datetime object or None."""
        if not value or value == '':
            return None
        try:
            # Try with seconds
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except:
            try:
                # Try without seconds
                return datetime.strptime(value, '%Y-%m-%d %H:%M')
            except:
                return None
    
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('booking_id')),
            parse_int(row.get('style_id')),
            row.get('boat_id'),
            row.get('time_id'),
            parse_int(row.get('timeframe_id')),
            parse_int(row.get('main_boat')),
            parse_int(row.get('num_passengers')),
            convert_timestamp(row.get('boat_departure')),
            convert_timestamp(row.get('boat_return')),
            row.get('status'),
            parse_float(row.get('price')),
            parse_int(row.get('price_override')),
            convert_timestamp(row.get('signature_date')),
            convert_timestamp(row.get('checkout_date')),
            row.get('checkout_equipment'),
            row.get('checkout_notes'),
            parse_float(row.get('checkout_engine_hours')),
            convert_timestamp(row.get('checkin_date')),
            row.get('checkin_equipment'),
            row.get('checkin_notes'),
            parse_float(row.get('checkin_engine_hours')),
            parse_float(row.get('checkin_hours')),
            parse_float(row.get('checkin_deposit')),
            parse_float(row.get('checkin_weather')),
            parse_float(row.get('checkin_late')),
            parse_float(row.get('checkin_miscnontax')),
            parse_float(row.get('checkin_misctax')),
            parse_float(row.get('checkin_cleaning')),
            parse_float(row.get('checkin_gallons')),
            parse_float(row.get('checkin_fuel')),
            parse_float(row.get('checkin_diesel_gallons')),
            parse_float(row.get('checkin_diesel')),
            parse_float(row.get('checkin_tip')),
            parse_float(row.get('checkin_tax_1')),
            parse_float(row.get('checkin_tax_2')),
            parse_float(row.get('checkin_total')),
            parse_int(row.get('queue_admin_id')),
            convert_timestamp(row.get('queue_date')),
            parse_int(row.get('attendant_queue_admin_id')),
            parse_int(row.get('attendant_water_admin_id')),
            parse_int(row.get('boat_assigned')),
            parse_int(row.get('addl_drivers')),
            row.get('addl_driver_names'),
            parse_int(row.get('accessories_migrated')),
            parse_int(row.get('price_rule_id')),
            parse_float(row.get('price_rule_original_price')),
            parse_float(row.get('price_rule_dynamic_price')),
            parse_float(row.get('price_rule_difference')),
            row.get('emergencyname'),
            row.get('emergencyphone'),
            row.get('dob'),
            row.get('contract_return_pdf'),
            row.get('contract_pdf'),
            convert_timestamp(row.get('created_at')),
            convert_timestamp(row.get('updated_at')),
            convert_timestamp(row.get('deleted_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} booking boat records")
    return data_rows


def parse_booking_payments_data(csv_content):
    """Parse booking_payments CSV - 56 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('booking_id')),
            parse_int(row.get('customer_id')),
            parse_int(row.get('admin_id')),
            parse_int(row.get('frontend')),
            row.get('payment_for'),
            row.get('payment_type'),
            row.get('card_type'),
            parse_float(row.get('payment_total')),
            parse_float(row.get('cash_total')),
            parse_float(row.get('credit_total')),
            parse_float(row.get('agent_ar_total')),
            row.get('credit_last4'),
            row.get('credit_expiry'),
            row.get('billing_firstname'),
            row.get('billing_lastname'),
            row.get('billing_street1'),
            row.get('billing_street2'),
            row.get('billing_city'),
            row.get('billing_state'),
            row.get('billing_country'),
            row.get('billing_zip'),
            row.get('transid'),
            parse_int(row.get('original_payment_id')),
            row.get('status'),
            row.get('notes'),
            row.get('is_agent_ar'),
            row.get('offline_type'),
            row.get('dockmaster_ticket'),
            row.get('mytaskit_id'),
            parse_float(row.get('report_boats')),
            parse_float(row.get('report_propane')),
            parse_float(row.get('report_accessories')),
            parse_float(row.get('report_parking')),
            parse_float(row.get('report_insurance')),
            parse_float(row.get('report_fuel')),
            parse_float(row.get('report_damages')),
            parse_float(row.get('report_cleaning')),
            parse_float(row.get('report_late')),
            parse_float(row.get('report_other')),
            parse_float(row.get('report_discount')),
            parse_float(row.get('internal_application_fee')),
            parse_float(row.get('cc_processor_fee')),
            row.get('cc_brand'),
            row.get('cc_country'),
            row.get('cc_funding'),
            row.get('cc_connect_type'),
            row.get('cc_connect_id'),
            row.get('cc_payout_id'),
            parse_date(row.get('cc_payout_date')),
            row.get('external_charge_id'),
            parse_int(row.get('is_synced')),
            row.get('stripe_reader_id'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at')),
            parse_date(row.get('deleted_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} booking payment records")
    return data_rows


def parse_booking_accessories_data(csv_content):
    """Parse booking_accessories CSV - 8 columns (no ID, composite key)."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('booking_id')),
            parse_int(row.get('accessory_id')),
            parse_int(row.get('qty')),
            parse_float(row.get('price')),
            parse_int(row.get('price_override')),
            parse_int(row.get('accessory_option_id')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} booking accessory records")
    return data_rows


def parse_style_groups_data(csv_content):
    """Parse style_groups CSV - 11 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('group_name'),
            parse_int(row.get('frontend_max_same_departures')),
            row.get('safety_test_enabled'),
            row.get('safety_test_instructions'),
            parse_float(row.get('safety_test_min_percent_pass')),
            parse_int(row.get('safety_test_expiration_days')),
            row.get('safety_video_link'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} style group records")
    return data_rows


def parse_styles_data(csv_content):
    """Parse styles CSV - 98 columns (matches database schema exactly)."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            parse_int(row.get('style_group_id')),
            row.get('style_name'),
            row.get('backend_display'),
            parse_int(row.get('position')),
            parse_float(row.get('turnaround_time')),
            parse_float(row.get('deposit_amount')),
            parse_float(row.get('multiday_deposit_amount')),
            parse_float(row.get('preauth_amount')),
            parse_float(row.get('fuel_burn_ratio')),
            parse_float(row.get('tax_1_rate')),
            parse_float(row.get('tax_2_rate')),
            row.get('insurance_enabled'),
            row.get('insurance_pricing_type'),
            parse_float(row.get('insurance_pricing_rate')),
            parse_float(row.get('insurance_first_day_price')),
            row.get('gratuity_enabled'),
            parse_float(row.get('gratuity_pricing_rate')),
            parse_float(row.get('parking_qty_multiplier')),
            row.get('frontend_display'),
            row.get('frontend_name'),
            parse_int(row.get('frontend_position')),
            row.get('frontend_type'),
            parse_int(row.get('frontend_qty_limit')),
            row.get('frontend_unit_selector'),
            row.get('frontend_partial_payment_type'),
            parse_float(row.get('frontend_partial_payment_amount')),
            row.get('backend_multi_day_disabled'),
            parse_int(row.get('max_same_style_per_booking')),
            parse_float(row.get('frontend_min_hours_advance_departure')),
            row.get('backend_hourly_enabled'),
            parse_float(row.get('weekday_backend_hourly_min_hours')),
            parse_float(row.get('weekday_backend_hourly_max_hours')),
            parse_float(row.get('weekend_backend_hourly_min_hours')),
            parse_float(row.get('weekend_backend_hourly_max_hours')),
            parse_float(row.get('holiday_backend_hourly_min_hours')),
            parse_float(row.get('holiday_backend_hourly_max_hours')),
            row.get('frontend_hourly_enabled'),
            parse_float(row.get('weekday_frontend_hourly_min_hours')),
            parse_float(row.get('weekday_frontend_hourly_max_hours')),
            parse_float(row.get('weekday_frontend_hourly_time_increment')),
            parse_float(row.get('weekday_frontend_hourly_length_increment')),
            parse_float(row.get('weekend_frontend_hourly_min_hours')),
            parse_float(row.get('weekend_frontend_hourly_max_hours')),
            parse_float(row.get('weekend_frontend_hourly_time_increment')),
            parse_float(row.get('weekend_frontend_hourly_length_increment')),
            parse_float(row.get('holiday_frontend_hourly_min_hours')),
            parse_float(row.get('holiday_frontend_hourly_max_hours')),
            parse_float(row.get('holiday_frontend_hourly_time_increment')),
            parse_float(row.get('holiday_frontend_hourly_length_increment')),
            row.get('backend_nightly_enabled'),
            parse_float(row.get('backend_nightly_min_nights')),
            parse_float(row.get('backend_nightly_max_nights')),
            parse_float(row.get('backend_nightly_start')),
            parse_float(row.get('backend_nightly_end')),
            parse_int(row.get('backend_nightly_discount_days')),
            row.get('backend_nightly_discount_type'),
            parse_float(row.get('backend_nightly_discount_amount')),
            row.get('frontend_nightly_enabled'),
            parse_float(row.get('frontend_nightly_min_nights')),
            parse_float(row.get('frontend_nightly_min_nights_peak')),
            parse_float(row.get('frontend_nightly_max_nights')),
            parse_float(row.get('frontend_nightly_start')),
            parse_float(row.get('frontend_nightly_end')),
            row.get('frontend_nightly_addl_times'),
            parse_int(row.get('frontend_nightly_discount_days')),
            row.get('frontend_nightly_discount_type'),
            parse_float(row.get('frontend_nightly_discount_amount')),
            row.get('image'),
            parse_int(row.get('passengers')),
            parse_int(row.get('weight_capacity')),
            parse_int(row.get('horsepower')),
            row.get('engine_type'),
            parse_float(row.get('length')),
            parse_float(row.get('width')),
            parse_float(row.get('draft')),
            parse_int(row.get('fuel_capacity')),
            row.get('brand'),
            row.get('model'),
            row.get('title'),
            row.get('description'),
            row.get('summary'),
            row.get('notes'),
            row.get('video_link'),
            row.get('smartwaiver_waiver_link'),
            row.get('accounting_item_id'),
            row.get('local_video_link'),
            row.get('dockmaster_part_number'),
            row.get('dockmaster_tax_code'),
            row.get('end_hours'),
            parse_float(row.get('seasonal_buffer_default_lower')),
            parse_float(row.get('seasonal_buffer_default_upper')),
            parse_float(row.get('seasonal_buffer_peak_lower')),
            parse_float(row.get('seasonal_buffer_peak_upper')),
            row.get('billable_unit_type'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} style records")
    return data_rows


def parse_style_boats_data(csv_content):
    """Parse style_boats CSV - 39 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('style_id')),
            row.get('boat_number'),
            row.get('paperless_number'),
            row.get('motor'),
            row.get('manufacturer'),
            row.get('serial'),
            row.get('in_fleet'),
            row.get('hull_number'),
            row.get('state_number'),
            row.get('cylinders'),
            row.get('hp'),
            row.get('model'),
            row.get('type'),
            parse_date(row.get('purchased_date')),
            parse_float(row.get('purchased_cost')),
            parse_date(row.get('sale_date')),
            parse_float(row.get('sale_price')),
            row.get('club_location'),
            row.get('dealer_name'),
            row.get('dealer_city'),
            row.get('dealer_state'),
            row.get('po_number'),
            row.get('boat_year_model'),
            row.get('motor_year_model'),
            row.get('motor_manufacturer_model'),
            parse_date(row.get('state_reg_date')),
            parse_date(row.get('state_reg_exp_date')),
            parse_float(row.get('engine_purchased_cost')),
            row.get('backend_display'),
            parse_int(row.get('position')),
            row.get('status'),
            parse_date(row.get('service_start')),
            parse_date(row.get('service_end')),
            row.get('clean_status'),
            row.get('insurance_reg_no'),
            row.get('buoy_insurance_status'),
            parse_date(parse_date(row.get('created_at'))),
            parse_date(parse_date(row.get('updated_at')))
        ))
    
    logger.info(f"Parsed {len(data_rows)} style boat records")
    return data_rows


def parse_customer_boats_data(csv_content):
    """Parse customer_boats CSV - 9 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('customer_id')),
            parse_int(row.get('slip_id')),
            row.get('boat_name'),
            row.get('boat_number'),
            parse_float(row.get('length')),
            parse_float(row.get('width')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} customer boat records")
    return data_rows


def parse_season_dates_data(csv_content):
    """Parse season_dates CSV - 4 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('season_id')),
            parse_date(row.get('start_date')),
            parse_date(row.get('end_date'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} season date records")
    return data_rows


def parse_style_hourly_prices_data(csv_content):
    """Parse style_hourly_prices CSV - 22 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('style_id')),
            parse_int(row.get('season_id')),
            row.get('hourly_type'),
            parse_float(row.get('default_price')),
            parse_float(row.get('holiday')),
            parse_float(row.get('saturday')),
            parse_float(row.get('sunday')),
            parse_float(row.get('monday')),
            parse_float(row.get('tuesday')),
            parse_float(row.get('wednesday')),
            parse_float(row.get('thursday')),
            parse_float(row.get('friday')),
            parse_float(row.get('day_discount')),
            parse_float(row.get('under_one_hour')),
            parse_float(row.get('first_hour_am')),
            parse_float(row.get('first_hour_pm')),
            parse_float(row.get('max_price')),
            parse_float(row.get('min_hours')),
            parse_float(row.get('max_hours')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} style hourly price records")
    return data_rows


def parse_style_times_data(csv_content):
    """Parse style_times CSV - 26 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('style_id')),
            parse_int(row.get('season_id')),
            row.get('description'),
            row.get('frontend_display'),
            parse_float(row.get('start_1')),
            parse_float(row.get('end_1')),
            parse_int(row.get('end_days_1')),
            row.get('status_1'),
            parse_float(row.get('start_2')),
            parse_float(row.get('end_2')),
            parse_int(row.get('end_days_2')),
            row.get('status_2'),
            parse_float(row.get('start_3')),
            parse_float(row.get('end_3')),
            parse_int(row.get('end_days_3')),
            row.get('status_3'),
            parse_float(row.get('start_4')),
            parse_float(row.get('end_4')),
            parse_int(row.get('end_days_4')),
            row.get('status_4'),
            row.get('valid_days'),
            parse_int(row.get('holidays_only_if_valid_day')),
            parse_int(row.get('mapped_time_id')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} style time records")
    return data_rows


def parse_style_prices_data(csv_content):
    """Parse style_prices CSV - 12 columns, uses TIME_ID as PK (not ID)."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('time_id')),
            parse_float(row.get('default_price')),
            parse_float(row.get('holiday')),
            parse_float(row.get('saturday')),
            parse_float(row.get('sunday')),
            parse_float(row.get('monday')),
            parse_float(row.get('tuesday')),
            parse_float(row.get('wednesday')),
            parse_float(row.get('thursday')),
            parse_float(row.get('friday')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} style price records")
    return data_rows


def parse_club_tiers_data(csv_content):
    """Parse club_tiers CSV - 28 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('tier_name'),
            row.get('frontend_display'),
            row.get('frontend_name'),
            parse_int(row.get('frontend_position')),
            parse_int(row.get('term_length')),
            row.get('term_length_type'),
            row.get('term_auto_renew'),
            parse_float(row.get('term_fee')),
            parse_int(row.get('period_length')),
            row.get('period_length_type'),
            parse_float(row.get('credits_per_period')),
            parse_float(row.get('hours_per_credit')),
            parse_float(row.get('period_fee')),
            row.get('frontend_display_pricing'),
            parse_float(row.get('no_show_fee')),
            row.get('allow_self_cancellations'),
            parse_float(row.get('cancellation_fee')),
            parse_float(row.get('application_fee')),
            parse_float(row.get('bdrd')),
            parse_int(row.get('max_pending_waitlist_entries')),
            row.get('free_accessories'),
            row.get('description'),
            row.get('terms'),
            row.get('status'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} club tier records")
    return data_rows


def parse_coupons_data(csv_content):
    """Parse coupons CSV - 30 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('code'),
            row.get('coupon_name'),
            row.get('coupon_type'),
            parse_float(row.get('coupon_amount')),
            parse_int(row.get('count_allowed')),
            parse_int(row.get('count_allowed_daily')),
            parse_int(row.get('count_used')),
            parse_date(row.get('rental_start')),
            parse_date(row.get('rental_end')),
            parse_date(row.get('coupon_start')),
            parse_date(row.get('coupon_end')),
            parse_float(row.get('min_departure_time')),
            parse_float(row.get('max_departure_time')),
            parse_float(row.get('min_return_time')),
            parse_float(row.get('max_return_time')),
            parse_float(row.get('min_hours')),
            parse_float(row.get('max_hours')),
            parse_float(row.get('min_hours_before_departure')),
            parse_float(row.get('max_hours_before_departure')),
            parse_int(row.get('max_same_day_per_customer')),
            parse_int(row.get('max_active_per_customer')),
            row.get('disable_consecutive_per_customer'),
            row.get('status'),
            row.get('valid_days'),
            parse_int(row.get('holidays_only_if_valid_day')),
            row.get('valid_styles'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} coupon records")
    return data_rows


def parse_pos_items_data(csv_content):
    """Parse pos_items CSV - 9 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('sku'),
            row.get('item_name'),
            parse_float(row.get('cost')),
            parse_float(row.get('price')),
            row.get('tax_exempt'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} POS item records")
    return data_rows


def parse_pos_sales_data(csv_content):
    """Parse pos_sales CSV - 11 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            parse_int(row.get('admin_id')),
            row.get('customer_name'),
            parse_float(row.get('subtotal')),
            parse_float(row.get('tax_1')),
            parse_float(row.get('grand_total')),
            parse_float(row.get('amount_paid')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at')),
            parse_date(row.get('deleted_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} POS sale records")
    return data_rows


def parse_fuel_sales_data(csv_content):
    """Parse fuel_sales CSV - 14 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            parse_int(row.get('admin_id')),
            row.get('customer_name'),
            row.get('fuel_type'),
            parse_float(row.get('qty')),
            parse_float(row.get('price')),
            parse_float(row.get('subtotal')),
            parse_float(row.get('tip')),
            parse_float(row.get('grand_total')),
            parse_float(row.get('amount_paid')),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at')),
            parse_date(row.get('deleted_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} fuel sale records")
    return data_rows


def parse_waitlists_data(csv_content):
    """Parse waitlists CSV - 18 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            parse_int(row.get('category_id')),
            parse_int(row.get('style_id')),
            parse_int(row.get('customer_id')),
            parse_int(row.get('time_id')),
            parse_int(row.get('timeframe_id')),
            row.get('firstname'),
            row.get('lastname'),
            row.get('email'),
            row.get('phone'),
            parse_date(row.get('departure')),
            row.get('length'),
            row.get('waitlist_time'),
            row.get('fulfilled'),
            parse_date(row.get('fulfilled_date')),
            parse_date(parse_date(row.get('created_at'))),
            parse_date(parse_date(row.get('updated_at')))
        ))
    
    logger.info(f"Parsed {len(data_rows)} waitlist records")
    return data_rows


def parse_closed_dates_data(csv_content):
    """Parse closed_dates CSV - 9 columns matching actual CSV structure."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            parse_date(row.get('closed_date')),
            row.get('allow_backend_departures'),
            row.get('allow_backend_returns'),
            row.get('allow_frontend_departures'),
            row.get('allow_frontend_returns'),
            parse_date(row.get('created_at')),
            parse_date(row.get('updated_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} closed date records")
    return data_rows


def parse_blacklists_data(csv_content):
    """Parse blacklists CSV - 10 columns (no updated_at in CSV)."""
    reader = csv.DictReader(io.StringIO(csv_content))
    data_rows = []
    
    for row in reader:
        data_rows.append((
            parse_int(row.get('id')),
            parse_int(row.get('location_id')),
            row.get('firstname'),
            row.get('lastname'),
            row.get('phone'),
            row.get('cell'),
            row.get('email'),
            row.get('dl_number'),
            row.get('notes'),
            parse_date(row.get('created_at'))
        ))
    
    logger.info(f"Parsed {len(data_rows)} blacklist records")
    return data_rows


def find_latest_data_file_in_s3(s3_client, bucket):
    """
    Find the most recent -DATA.sql.gz file in S3 bucket root.
    Files are named like: prod_resilient_2025-10-01_16_03-DATA.sql.gz
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket)
        
        if 'Contents' not in response:
            logger.warning(f"No files found in bucket: {bucket}")
            return None
        
        # Filter for -DATA.sql.gz files only
        data_files = [obj for obj in response['Contents'] 
                      if obj['Key'].endswith('-DATA.sql.gz')]
        
        if not data_files:
            logger.warning(f"No -DATA.sql.gz files found in bucket: {bucket}")
            return None
        
        # Sort by last modified date (most recent first)
        data_files.sort(key=lambda x: x['LastModified'], reverse=True)
        latest_file = data_files[0]
        
        logger.info(f"Found latest DATA file: {latest_file['Key']}")
        logger.info(f"  Last Modified: {latest_file['LastModified']}")
        logger.info(f"  Size: {latest_file['Size']:,} bytes")
        return latest_file['Key']
        
    except Exception as e:
        logger.error(f"Error finding latest DATA file: {e}")
        return None


def extract_table_data_from_sql(sql_content, table_name):
    """
    Extract INSERT statements for a specific table from SQL dump.
    Returns list of tuples containing the data rows.
    """
    import re
    
    logger.info(f"Extracting data for table: {table_name.upper()}")
    
    # Pattern to match INSERT INTO statements for this table
    # Example: INSERT INTO `customers` VALUES (1,'John',...);
    pattern = rf"INSERT INTO `{table_name}` VALUES\s*\((.*?)\);"
    
    matches = re.findall(pattern, sql_content, re.IGNORECASE | re.DOTALL)
    
    if not matches:
        logger.warning(f"No INSERT statements found for table: {table_name}")
        return []
    
    data_rows = []
    for match in matches:
        # Split by comma, but respect quotes and parentheses
        # This is a simplified parser - may need enhancement
        values = []
        current_value = ""
        in_quotes = False
        paren_depth = 0
        
        for char in match + ',':
            if char == "'" and (not current_value or current_value[-1] != '\\'):
                in_quotes = not in_quotes
                current_value += char
            elif char == '(' and not in_quotes:
                paren_depth += 1
                current_value += char
            elif char == ')' and not in_quotes:
                paren_depth -= 1
                current_value += char
            elif char == ',' and not in_quotes and paren_depth == 0:
                # End of value
                val = current_value.strip()
                # Remove quotes and handle NULL
                if val.upper() == 'NULL':
                    values.append(None)
                elif val.startswith("'") and val.endswith("'"):
                    # Remove quotes and unescape
                    values.append(val[1:-1].replace("\\'", "'").replace("\\\\", "\\"))
                else:
                    # Try to convert to int or float
                    try:
                        if '.' in val:
                            values.append(float(val))
                        else:
                            values.append(int(val))
                    except:
                        values.append(val)
                current_value = ""
            else:
                current_value += char
        
        if values:
            data_rows.append(tuple(values))
    
    logger.info(f"Extracted {len(data_rows)} rows from {table_name}")
    return data_rows


def process_stellar_data_from_s3(
    bucket,
    region,
    db_user,
    db_password,
    db_dsn,
    aws_access_key_id=None,
    aws_secret_access_key=None
):
    """
    Main Stellar data processing function.
    Downloads gzipped tarball from S3, extracts CSV files,
    and inserts into Oracle staging tables.
    
    The backup file is a .tar.gz containing CSV files in data/ directory.
    Example: prod_resilient_2025-11-12_09_52-DATA.sql.gz
    """
    import tarfile
    import gzip as gzip_module
    
    logger.info("=" * 80)
    logger.info("STELLAR BUSINESS DATA PROCESSING - START")
    logger.info("=" * 80)
    
    # Initialize S3 client
    try:
        if aws_access_key_id and aws_secret_access_key:
            s3_client = boto3.client(
                's3',
                region_name=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        else:
            s3_client = boto3.client('s3', region_name=region)
        
        logger.info(f"Connected to S3 region: {region}, bucket: {bucket}")
        
    except Exception as e:
        logger.exception(f"Failed to initialize S3 client: {e}")
        raise
    
    # Find and download latest DATA file
    latest_file = find_latest_data_file_in_s3(s3_client, bucket)
    if not latest_file:
        logger.error("No DATA file found in S3 bucket")
        return
    
    try:
        logger.info(f"Downloading: s3://{bucket}/{latest_file}")
        response = s3_client.get_object(Bucket=bucket, Key=latest_file)
        
        # Decompress gzip
        with gzip_module.GzipFile(fileobj=response['Body']) as gzipfile:
            tar_content = gzipfile.read()
        
        logger.info(f"Downloaded and decompressed {len(tar_content):,} bytes")
        
        # Extract tarball
        tar_like = io.BytesIO(tar_content)
        tar = tarfile.open(fileobj=tar_like)
        
        logger.info(f"Tarball contains {len(tar.getnames())} files")
        
    except Exception as e:
        logger.exception(f"Failed to download/extract tarball: {e}")
        raise
    
    # Initialize Oracle database
    try:
        db_connector = OracleConnector(db_user, db_password, db_dsn)
        logger.info("Connected to Oracle database")
    except Exception as e:
        logger.exception(f"Failed to connect to Oracle: {e}")
        raise
    
    # Define tables to process with their insert functions and parsers
    tables_to_process = [
        ('customers', parse_customers_data, db_connector.insert_customers),
        ('locations', parse_locations_data, db_connector.insert_locations),
        ('seasons', parse_seasons_data, db_connector.insert_seasons),
        ('accessories', parse_accessories_data, db_connector.insert_accessories),
        ('accessory_options', parse_accessory_options_data,
         db_connector.insert_accessory_options),
        ('accessory_tiers', parse_accessory_tiers_data,
         db_connector.insert_accessory_tiers),
        ('amenities', parse_amenities_data, db_connector.insert_amenities),
        ('categories', parse_categories_data, db_connector.insert_categories),
        ('holidays', parse_holidays_data, db_connector.insert_holidays),
        ('bookings', parse_bookings_data, db_connector.insert_bookings),
        ('booking_boats', parse_booking_boats_data,
         db_connector.insert_booking_boats),
        ('booking_payments', parse_booking_payments_data,
         db_connector.insert_booking_payments),
        ('booking_accessories', parse_booking_accessories_data,
         db_connector.insert_booking_accessories),
        ('style_groups', parse_style_groups_data,
         db_connector.insert_style_groups),
        ('styles', parse_styles_data, db_connector.insert_styles),
        ('style_boats', parse_style_boats_data,
         db_connector.insert_style_boats),
        ('customer_boats', parse_customer_boats_data,
         db_connector.insert_customer_boats),
        ('season_dates', parse_season_dates_data,
         db_connector.insert_season_dates),
        ('style_hourly_prices', parse_style_hourly_prices_data,
         db_connector.insert_style_hourly_prices),
        ('style_times', parse_style_times_data,
         db_connector.insert_style_times),
        ('style_prices', parse_style_prices_data,
         db_connector.insert_style_prices),
        ('club_tiers', parse_club_tiers_data, db_connector.insert_club_tiers),
        ('coupons', parse_coupons_data, db_connector.insert_coupons),
        ('pos_items', parse_pos_items_data, db_connector.insert_pos_items),
        ('pos_sales', parse_pos_sales_data, db_connector.insert_pos_sales),
        ('fuel_sales', parse_fuel_sales_data, db_connector.insert_fuel_sales),
        ('waitlists', parse_waitlists_data, db_connector.insert_waitlists),
        ('closed_dates', parse_closed_dates_data,
         db_connector.insert_closed_dates),
        ('blacklists', parse_blacklists_data, db_connector.insert_blacklists),
    ]
    
    # Process each table
    total_records = 0
    successful_tables = 0
    successful_tables_details = {}  # Track successful tables with record counts
    failed_tables = []
    failed_tables_details = {}  # Track error details for each failed table
    
    for table_name, parser_func, insert_func in tables_to_process:
        try:
            logger.info(f"\nProcessing table: {table_name.upper()}")
            
            # Look for CSV file in tarball
            csv_filename = f"data/{table_name}.csv"
            try:
                csv_file = tar.extractfile(csv_filename)
                if csv_file is None:
                    logger.warning(f"File not found in tarball: {csv_filename}")
                    failed_tables.append(table_name)
                    continue
                
                csv_content = csv_file.read().decode('utf-8', errors='ignore')
                logger.info(f"Extracted {len(csv_content):,} bytes from {csv_filename}")
                
            except KeyError:
                logger.warning(f"File not found in tarball: {csv_filename}")
                failed_tables.append(table_name)
                continue
            
            # Parse CSV content
            data_rows = parser_func(csv_content)
            
            if data_rows:
                staging_table = f"STG_STELLAR_{table_name.upper()}"
                logger.info(f"Truncating {staging_table}...")
                db_connector.cursor.execute(
                    f"TRUNCATE TABLE {staging_table}"
                )
                db_connector.connection.commit()
                
                insert_func(data_rows)
                
                total_records += len(data_rows)
                successful_tables += 1
                successful_tables_details[table_name] = len(data_rows)
                logger.info(
                    f"✅ Successfully processed {table_name}: "
                    f"{len(data_rows)} records"
                )
            else:
                logger.warning(f"No data rows parsed for {table_name}")
                failed_tables.append(table_name)
                failed_tables_details[table_name] = "No data rows in CSV file"
                
        except Exception as e:
            error_msg = str(e)
            logger.exception(f"Failed to process {table_name}: {e}")
            failed_tables.append(table_name)
            
            # Categorize the error for the report
            if 'ORA-01843' in error_msg:
                failed_tables_details[table_name] = "Date format error"
            elif 'ORA-01400' in error_msg:
                # Extract column name from error
                import re
                match = re.search(r'"([^"]+)"', error_msg)
                col = match.group(1) if match else "unknown column"
                failed_tables_details[table_name] = f"NULL constraint: {col}"
            elif 'ORA-00932' in error_msg:
                failed_tables_details[table_name] = "Data type mismatch"
            elif 'ORA-01036' in error_msg:
                failed_tables_details[table_name] = "Bind variable count mismatch"
            else:
                failed_tables_details[table_name] = error_msg[:100]
    
    # Close tarball and connection
    try:
        tar.close()
    except Exception as e:
        logger.warning(f"Error closing tarball: {e}")
    
    try:
        db_connector.cursor.close()
        db_connector.connection.close()
        logger.info("Database connection closed")
    except Exception as e:
        logger.warning(f"Error closing connection: {e}")
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("STELLAR BUSINESS DATA PROCESSING - SUMMARY")
    logger.info("=" * 80)
    logger.info(
        f"Successfully processed: {successful_tables}/"
        f"{len(tables_to_process)} tables"
    )
    logger.info(f"Total records loaded: {total_records}")
    
    if failed_tables:
        logger.warning(f"Failed tables: {', '.join(failed_tables)}")
    else:
        logger.info("All tables processed successfully!")
    
    logger.info("=" * 80)
    
    # Return processing results for caller to display
    return {
        'successful_tables': successful_tables_details,
        'failed_tables': failed_tables_details,
        'total_records': total_records,
        'total_tables': len(tables_to_process)
    }
