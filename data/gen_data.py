#!/usr/bin/env python3
"""
Healthcare Database Test Data Generator
This script generates approximately 1 million doctor records, 1,000 practice records,
and appropriate relationships between them for PostgreSQL.
"""

import psycopg2
import random
import string
import datetime
import time
import uuid
from faker import Faker
from tqdm import tqdm

# Initialize Faker with non-random seed to avoid duplicates
fake = Faker()
# Don't set a seed to ensure randomness across runs
random.seed(None)

# Configuration
DB_CONFIG = {
    "dbname": "interview"
    # No user, password or host specified to use local Unix socket with current system user
}

# Counts
NUM_DOCTORS = 1000_000
NUM_PRACTICES = 1_000
MAX_LICENSES_PER_DOCTOR = 3
MAX_PRACTICES_PER_DOCTOR = 3

# Specialties for doctors
SPECIALTIES = [
    "Family Medicine", "Internal Medicine", "Pediatrics", "Cardiology", 
    "Dermatology", "Endocrinology", "Gastroenterology", "Neurology", 
    "Obstetrics and Gynecology", "Oncology", "Ophthalmology", "Orthopedics",
    "Psychiatry", "Radiology", "Surgery", "Urology", "Emergency Medicine",
    "Anesthesiology", "Pathology", "Physical Medicine", "Rheumatology"
]

# License types
LICENSE_TYPES = ["Medical License", "DEA", "Board Certification", "State Controlled Substance"]

# States
STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", 
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# License statuses
LICENSE_STATUSES = ["Active", "Expired", "Suspended", "Revoked"]

# Roles for doctors in practices
ROLES = ["Primary Care Physician", "Specialist", "Consultant", "Department Head", 
         "Medical Director", "Attending Physician", "Resident", "Fellow"]

def connect_to_db():
    """Establish a connection to the database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        exit(1)

def clear_tables(conn):
    """Clear existing data from tables"""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE doctor_practices, licenses, doctors, practices RESTART IDENTITY CASCADE;")
    conn.commit()
    print("Existing data cleared.")

def generate_practices(conn):
    """Generate practice records"""
    practice_ids = []
    batch_size = 100
    practice_batch = []
    
    print("Generating practices...")
    for i in tqdm(range(NUM_PRACTICES)):
        practice_name = f"{fake.company()} {random.choice(['Medical Center', 'Clinic', 'Healthcare', 'Hospital', 'Medical Group', 'Physicians'])}"
        
        practice = (
            practice_name,
            fake.street_address(),
            fake.secondary_address() if random.random() < 0.3 else None,
            fake.city(),
            random.choice(STATES),
            fake.zipcode()[:10],
            fake.phone_number()[:20],
            fake.company_email()[:255],
            (f"www.{practice_name.lower().replace(' ', '')}.com")[:255] if random.random() < 0.8 else None
        )
        practice_batch.append(practice)
        
        if len(practice_batch) >= batch_size or i == NUM_PRACTICES - 1:
            with conn.cursor() as cur:
                args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", p).decode('utf-8') for p in practice_batch)
                cur.execute(f"""
                    INSERT INTO practices (practice_name, address_line1, address_line2, city, state, zip_code, phone, email, website)
                    VALUES {args} RETURNING practice_id;
                """)
                new_ids = [row[0] for row in cur.fetchall()]
                practice_ids.extend(new_ids)
            
            conn.commit()
            practice_batch = []
    
    print(f"Generated {len(practice_ids)} practices.")
    return practice_ids

def generate_doctors(conn):
    """Generate doctor records"""
    doctor_ids = []
    batch_size = 10000
    doctor_batch = []
    
    print("Generating doctors...")
    for i in tqdm(range(NUM_DOCTORS)):
        doctor = (
            fake.first_name(),
            fake.last_name(),
            random.choice(SPECIALTIES),
            fake.email(),
            fake.phone_number()[:20],
            fake.date_of_birth(minimum_age=25, maximum_age=70)
        )
        doctor_batch.append(doctor)
        
        if len(doctor_batch) >= batch_size or i == NUM_DOCTORS - 1:
            with conn.cursor() as cur:
                args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", d).decode('utf-8') for d in doctor_batch)
                cur.execute(f"""
                    INSERT INTO doctors (first_name, last_name, specialty, email, phone, date_of_birth)
                    VALUES {args} RETURNING doctor_id;
                """)
                new_ids = [row[0] for row in cur.fetchall()]
                doctor_ids.extend(new_ids)
            
            conn.commit()
            doctor_batch = []
            
    print(f"Generated {len(doctor_ids)} doctors.")
    return doctor_ids

def generate_licenses(conn, doctor_ids):
    """Generate license records for doctors"""
    license_batch = []
    batch_size = 100000
    licenses_count = 0
    
    print("Generating licenses...")
    for doctor_id in tqdm(doctor_ids):
        # Each doctor has 1-3 licenses
        num_licenses = random.randint(1, MAX_LICENSES_PER_DOCTOR)
        
        for _ in range(num_licenses):
            state = random.choice(STATES)
            issue_date = fake.date_between(start_date="-15y", end_date="today")
            expiry_date = issue_date + datetime.timedelta(days=random.randint(365*1, 365*10))
            
            license_info = (
                doctor_id,
                f"{state}-{fake.numerify('######')}-{fake.lexify('???')}",
                random.choice(LICENSE_TYPES),
                state,
                issue_date,
                expiry_date,
                random.choices(LICENSE_STATUSES, weights=[0.8, 0.15, 0.03, 0.02])[0]
            )
            license_batch.append(license_info)
            licenses_count += 1
            
            if len(license_batch) >= batch_size:
                with conn.cursor() as cur:
                    args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", l).decode('utf-8') for l in license_batch)
                    cur.execute(f"""
                        INSERT INTO licenses (doctor_id, license_number, license_type, state, issue_date, expiry_date, status)
                        VALUES {args};
                    """)
                
                conn.commit()
                license_batch = []
                
    # Insert any remaining batch
    if license_batch:
        with conn.cursor() as cur:
            args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", l).decode('utf-8') for l in license_batch)
            cur.execute(f"""
                INSERT INTO licenses (doctor_id, license_number, license_type, state, issue_date, expiry_date, status)
                VALUES {args};
            """)
        conn.commit()
    
    print(f"Generated {licenses_count} licenses.")

def generate_doctor_practices(conn, doctor_ids, practice_ids):
    """Associate doctors with practices"""
    doctor_practice_batch = []
    batch_size = 100000
    associations_count = 0
    
    print("Generating doctor-practice associations...")
    for doctor_id in tqdm(doctor_ids):
        # Each doctor works at 1-3 practices
        num_practices = random.randint(1, MAX_PRACTICES_PER_DOCTOR)
        doctor_practices = random.sample(practice_ids, min(num_practices, len(practice_ids)))
        
        for i, practice_id in enumerate(doctor_practices):
            start_date = fake.date_between(start_date="-10y", end_date="-1m")
            end_date = None if random.random() < 0.8 else fake.date_between(start_date=start_date, end_date="+2y")
            
            association = (
                doctor_id,
                practice_id,
                random.choice(ROLES),
                start_date,
                end_date,
                i == 0,  # First practice is primary for this doctor
                random.randint(4, 40)  # Hours per week
            )
            doctor_practice_batch.append(association)
            associations_count += 1
            
            if len(doctor_practice_batch) >= batch_size:
                with conn.cursor() as cur:
                    args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", a).decode('utf-8') for a in doctor_practice_batch)
                    cur.execute(f"""
                        INSERT INTO doctor_practices (doctor_id, practice_id, role, start_date, end_date, is_primary, hours_per_week)
                        VALUES {args};
                    """)
                
                conn.commit()
                doctor_practice_batch = []
    
    # Insert any remaining batch
    if doctor_practice_batch:
        with conn.cursor() as cur:
            args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", a).decode('utf-8') for a in doctor_practice_batch)
            cur.execute(f"""
                INSERT INTO doctor_practices (doctor_id, practice_id, role, start_date, end_date, is_primary, hours_per_week)
                VALUES {args};
            """)
        conn.commit()
    
    print(f"Generated {associations_count} doctor-practice associations.")

def create_indexes(conn):
    """Create additional indexes for performance"""
    print("Creating additional indexes...")
    with conn.cursor() as cur:
        # These indexes are already in the schema, but if they weren't, we'd add them here
        # cur.execute("CREATE INDEX IF NOT EXISTS idx_licenses_doctor_id ON licenses(doctor_id);")
        # cur.execute("CREATE INDEX IF NOT EXISTS idx_doctor_practices_doctor_id ON doctor_practices(doctor_id);")
        # cur.execute("CREATE INDEX IF NOT EXISTS idx_doctor_practices_practice_id ON doctor_practices(practice_id);")
        
        # Additional indexes that might be useful
        cur.execute("CREATE INDEX IF NOT EXISTS idx_practices_name ON practices(practice_name);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_licenses_state ON licenses(state);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_licenses_status ON licenses(status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_doctors_specialty ON doctors(specialty);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_doctor_practices_end_date ON doctor_practices(end_date);")
    conn.commit()
    print("Additional indexes created.")

def main():
    """Main function to generate test data"""
    start_time = time.time()
    
    print("Connecting to database...")
    conn = connect_to_db()
    
    # Ask for confirmation before clearing data
    answer = input("This will clear existing data in the healthcare database tables. Continue? (y/n): ")
    if answer.lower() != 'y':
        print("Operation cancelled.")
        conn.close()
        return
    
    # Generate data
    clear_tables(conn)
    practice_ids = generate_practices(conn)
    doctor_ids = generate_doctors(conn)
    generate_licenses(conn, doctor_ids)
    generate_doctor_practices(conn, doctor_ids, practice_ids)
    # create_indexes(conn)
    
    # Close connection
    conn.close()
    
    elapsed_time = time.time() - start_time
    print(f"Data generation completed in {elapsed_time:.2f} seconds.")
    print(f"Generated {NUM_DOCTORS} doctors and {NUM_PRACTICES} practices with relationships.")

if __name__ == "__main__":
    main()
