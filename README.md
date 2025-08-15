# ===========================
# Fact_Visits Synthetic Data Generator
# - Creates 50-row sample CSV
# - Creates 500,000-row CSV in chunks
# Schema: flattened visit + dimension attributes
# Requires: pip install faker pandas
# ===========================

import os
import csv
import math
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd

# ---------------------------
# Config
# ---------------------------
SEED = 42
SAMPLE_ROWS = 50
TOTAL_ROWS = 500_000           # 5 lakh
CHUNK_SIZE = 50_000            # writes in 10 chunks
SAMPLE_CSV = "fact_visits_sample_50.csv"
FULL_CSV = "fact_visits_500k.csv"

# ---------------------------
# Faker & Random seeding
# ---------------------------
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# ---------------------------
# Reference/value pools
# ---------------------------
countries = ["India", "USA", "Canada", "UK", "Australia"]
in_states = ["Maharashtra", "Karnataka", "Delhi", "Gujarat", "Tamil Nadu", "Telangana", "West Bengal"]
us_states = ["Texas", "California", "New York", "Florida", "Washington", "Illinois"]
gb_regions = ["London", "Greater Manchester", "West Midlands", "West Yorkshire"]
au_states = ["New South Wales", "Victoria", "Queensland", "Western Australia"]
ca_provinces = ["Ontario", "Quebec", "British Columbia", "Alberta"]

city_pool = [
    "Pune", "Mumbai", "Bengaluru", "Hyderabad", "Delhi", "Chennai", "Kolkata",
    "Dallas", "Austin", "San Jose", "New York", "Toronto", "Vancouver",
    "London", "Sydney", "Melbourne"
]

genders = ["Male", "Female", "Other"]
specializations = [
    "Cardiology", "Neurology", "Orthopedics", "Pediatrics", "Oncology",
    "Dermatology", "General Medicine", "Radiology", "Anesthesiology", "ENT"
]
hospital_types = ["General", "Specialty", "Teaching", "Research"]
accreditations = ["NABH", "JCI", "ISO", "None"]
room_types = ["ICU", "General Ward", "Private Room", "Semi-Private", "ER Bay"]
units = ["ICU Unit", "Surgical Unit", "Maternity Unit", "ER Unit", "OPD Unit"]
shift_names = ["Morning", "Evening", "Night"]
diagnosis_categories = ["Chronic", "Acute", "Infectious", "Genetic", "Lifestyle"]
insurance_plans = ["Gold Plan", "Silver Plan", "Platinum Plan", "Basic Plan"]

# ---------------------------
# Schema / column order
# ---------------------------
COLUMNS = [
    "visit_id",
    "patient_id", "patient_fullname", "patient_gender", "age", "patient_dob",
    "patient_address", "patient_city", "patient_state", "patient_country", "patient_postalcode",
    "patient_admission_date", "patient_discharge_date", "patient_wait_time_minutes",
    "doctor_id", "doctor_fullname", "doctor_specialization", "doctor_contactnumber",
    "doctor_years_experience", "doctor_email", "doctor_shift_type",
    "department_id", "department_name", "dept_speciality", "floor_number",
    "hospital_id", "hospital_name", "hospital_type", "accreditation_level", "bed_capacity",
    "hospital_address", "hospital_state", "hospital_city", "hospital_country", "hospital_postalcode",
    "room_id", "room_number", "room_type", "floor_number_room",
    "unit_id", "unit_name",
    "shift_id", "shift_name", "shift_start_time", "shift_end_time",
    "total_billing_amount", "insurance_covered_amount", "patient_covered_amount",
    "full_date",
    "diagnosis_id", "diagnosis_code", "diagnosis_description", "diagnosis_category",
    "insuranceprovider_id", "insuranceprovider_name", "insuranceplan_type",
    "coverage_percentage", "insurance_contact_number", "insurance_email"
]

# ---------------------------
# Helpers
# ---------------------------
def pick_state_country():
    country = random.choice(countries)
    if country == "India":
        return random.choice(in_states), country
    if country == "USA":
        return random.choice(us_states), country
    if country == "UK":
        return random.choice(gb_regions), country
    if country == "Australia":
        return random.choice(au_states), country
    if country == "Canada":
        return random.choice(ca_provinces), country
    return "Unknown", country

def shift_bounds(name):
    if name == "Morning":
        return "08:00", "14:00"
    if name == "Evening":
        return "14:00", "20:00"
    return "20:00", "08:00"  # Night

def iso_date(d):
    return d.strftime("%Y-%m-%d")

def rand_postal(country):
    # Keep it simple & string-safe
    return fake.postcode().replace(" ", "")

def make_row(i: int):
    # Patient
    patient_id = f"P{i:06d}"
    patient_name = fake.name()
    patient_gender = random.choice(genders)
    dob = fake.date_of_birth(minimum_age=1, maximum_age=100)
    age = datetime.now().year - dob.year
    p_addr = fake.street_address()
    p_state, p_country = pick_state_country()
    p_city = random.choice(city_pool)
    p_postal = rand_postal(p_country)

    # Admission / discharge dates
    adm_date = fake.date_between(start_date="-365d", end_date="today")
    # discharge 0–7 days after admission (some OPD same-day)
    dis_date = adm_date + timedelta(days=random.randint(0, 7))
    wait_mins = random.randint(5, 120)

    # Doctor
    doctor_id = f"D{random.randint(1, 3000):05d}"
    doctor_name = f"Dr. {fake.name()}"
    spec = random.choice(specializations)
    doc_phone = fake.phone_number()
    doc_exp = random.randint(1, 40)
    doc_email = fake.email()
    doc_shift = random.choice(shift_names)

    # Department
    dept_id = f"DEP{random.randint(1, 200):04d}"
    dept_name = f"{spec} Dept"
    dept_spec = spec
    dept_floor = random.randint(1, 12)

    # Hospital
    hospital_id = f"H{random.randint(1, 200):04d}"
    hospital_name = fake.company() + " Hospital"
    hospital_type = random.choice(hospital_types)
    accreditation = random.choice(accreditations)
    beds = random.randint(50, 1200)
    h_addr = fake.street_address()
    h_state, h_country = pick_state_country()
    h_city = random.choice(city_pool)
    h_postal = rand_postal(h_country)

    # Room
    room_id = f"R{random.randint(1, 5000):05d}"
    room_no = str(random.randint(1, 999))
    room_type = random.choice(room_types)
    room_floor = random.randint(0, 12)

    # Unit
    unit_id = f"U{random.randint(1, 500):04d}"
    unit_name = random.choice(units)

    # Shift
    shift_id = f"S{random.randint(1, 100):04d}"
    sname = random.choice(shift_names)
    s_start, s_end = shift_bounds(sname)

    # Billing
    total_bill = random.randint(500, 200000)  # in currency units
    coverage_pct = random.randint(40, 95)
    insurance_covered = int(total_bill * coverage_pct / 100.0)
    patient_covered = total_bill - insurance_covered

    # Date for reporting
    full_date = adm_date

    # Diagnosis
    diag_id = f"DG{random.randint(1, 6000):05d}"
    diag_code = fake.bothify(text="???-##")  # e.g., ABC-12
    diag_desc = fake.sentence(nb_words=3).rstrip(".")
    diag_cat = random.choice(diagnosis_categories)

    # Insurance
    ins_id = f"IP{random.randint(1, 3000):05d}"
    ins_name = fake.company()
    ins_plan = random.choice(insurance_plans)
    ins_contact = fake.phone_number()
    ins_email = fake.company_email()

    return {
        "visit_id": i,
        "patient_id": patient_id,
        "patient_fullname": patient_name,
        "patient_gender": patient_gender,
        "age": age,
        "patient_dob": iso_date(dob),
        "patient_address": p_addr,
        "patient_city": p_city,
        "patient_state": p_state,
        "patient_country": p_country,
        "patient_postalcode": p_postal,
        "patient_admission_date": iso_date(adm_date),
        "patient_discharge_date": iso_date(dis_date),
        "patient_wait_time_minutes": wait_mins,
        "doctor_id": doctor_id,
        "doctor_fullname": doctor_name,
        "doctor_specialization": spec,
        "doctor_contactnumber": doc_phone,
        "doctor_years_experience": doc_exp,
        "doctor_email": doc_email,
        "doctor_shift_type": doc_shift,
        "department_id": dept_id,
        "department_name": dept_name,
        "dept_speciality": dept_spec,
        "floor_number": dept_floor,
        "hospital_id": hospital_id,
        "hospital_name": hospital_name,
        "hospital_type": hospital_type,
        "accreditation_level": accreditation,
        "bed_capacity": beds,
        "hospital_address": h_addr,
        "hospital_state": h_state,
        "hospital_city": h_city,
        "hospital_country": h_country,
        "hospital_postalcode": h_postal,
        "room_id": room_id,
        "room_number": room_no,
        "room_type": room_type,
        "floor_number_room": room_floor,
        "unit_id": unit_id,
        "unit_name": unit_name,
        "shift_id": shift_id,
        "shift_name": sname,
        "shift_start_time": s_start,
        "shift_end_time": s_end,
        "total_billing_amount": total_bill,
        "insurance_covered_amount": insurance_covered,
        "patient_covered_amount": patient_covered,
        "full_date": iso_date(full_date),
        "diagnosis_id": diag_id,
        "diagnosis_code": diag_code,
        "diagnosis_description": diag_desc,
        "diagnosis_category": diag_cat,
        "insuranceprovider_id": ins_id,
        "insuranceprovider_name": ins_name,
        "insuranceplan_type": ins_plan,
        "coverage_percentage": coverage_pct,
        "insurance_contact_number": ins_contact,
        "insurance_email": ins_email
    }

# ---------------------------
# Writers
# ---------------------------
def write_sample_csv():
    rows = [make_row(i) for i in range(1, SAMPLE_ROWS + 1)]
    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_csv(SAMPLE_CSV, index=False)
    print(f"[OK] Wrote sample: {SAMPLE_CSV} ({len(df)} rows)")

def write_full_csv():
    # Start fresh
    if os.path.exists(FULL_CSV):
        os.remove(FULL_CSV)

    # Write in chunks for memory efficiency
    written = 0
    next_id = 1
    passes = math.ceil(TOTAL_ROWS / CHUNK_SIZE)

    for p in range(passes):
        remaining = TOTAL_ROWS - written
        batch = min(CHUNK_SIZE, remaining)
        rows = [make_row(next_id + j) for j in range(batch)]
        df = pd.DataFrame(rows, columns=COLUMNS)

        # Write header only once
        df.to_csv(FULL_CSV, mode="a", index=False, header=(p == 0))
        written += batch
        next_id += batch
        print(f"[OK] Wrote chunk {p+1}/{passes}: +{batch} rows (total {written}/{TOTAL_ROWS})")

    print(f"[DONE] Wrote full dataset: {FULL_CSV} ({TOTAL_ROWS} rows)")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    write_sample_csv()
    write_full_csv()
