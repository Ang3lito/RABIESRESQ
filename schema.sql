-- RabiesResQ SQLite schema
-- Notes:
-- - All datetime/date/time values stored as TEXT (ISO 8601 recommended)
-- - JSON stored as TEXT (JSON string)
-- - Booleans stored as INTEGER 0/1 with CHECK constraints where helpful
-- - Create order respects FK dependencies

PRAGMA foreign_keys = ON;

BEGIN;

-- =========================
-- Core tables (clinics/users)
-- =========================

CREATE TABLE IF NOT EXISTS clinics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  address TEXT
);

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  email TEXT UNIQUE,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK(role IN ('patient','clinic_personnel','system_admin')),
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- Role tables
-- =========================

CREATE TABLE IF NOT EXISTS patients (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  first_name TEXT,
  last_name TEXT,
  phone_number TEXT,
  address TEXT,
  date_of_birth TEXT,
  age INTEGER,
  gender TEXT,
  allergies TEXT,
  pre_existing_conditions TEXT,
  current_medications TEXT,
  notification_settings TEXT,
  relationship_to_user TEXT NOT NULL DEFAULT 'Self',
  onboarding_completed INTEGER NOT NULL DEFAULT 0 CHECK(onboarding_completed IN (0,1)),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS system_admins (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL UNIQUE,
  first_name TEXT,
  last_name TEXT,
  employee_id TEXT UNIQUE,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS clinic_personnel (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL UNIQUE,
  clinic_id INTEGER NOT NULL,
  first_name TEXT,
  last_name TEXT,
  employee_id TEXT NOT NULL UNIQUE,
  license_number TEXT UNIQUE,
  title TEXT NOT NULL CHECK(title IN ('Doctor','Nurse')),
  specialty TEXT,
  phone_number TEXT,
  permissions_json TEXT,
  UNIQUE(clinic_id, user_id),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE RESTRICT
);

-- =========================
-- Clinical tables
-- =========================

CREATE TABLE IF NOT EXISTS cases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  patient_id INTEGER NOT NULL,
  clinic_id INTEGER NOT NULL,
  exposure_date TEXT NOT NULL,
  exposure_time TEXT,
  place_of_exposure TEXT,
  affected_area TEXT,
  type_of_exposure TEXT,
  animal_detail TEXT,
  animal_condition TEXT,
  category TEXT,
  risk_level TEXT NOT NULL,
  case_status TEXT,
  tetanus_prophylaxis_status TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE RESTRICT,
  FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE RESTRICT
);

-- 1:1 with cases, PK is FK
CREATE TABLE IF NOT EXISTS pre_screening_details (
  case_id INTEGER PRIMARY KEY,
  wound_description TEXT,
  bleeding_type TEXT,
  local_treatment TEXT,
  patient_prev_immunization TEXT,
  prev_vaccine_date TEXT,
  hrtig_immunization INTEGER CHECK(hrtig_immunization IN (0,1) OR hrtig_immunization IS NULL),
  hrtig_date TEXT,
  tetanus_date TEXT,
  pre_screening_score INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pre_screening_guidelines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  criterion_name TEXT NOT NULL,
  description TEXT NOT NULL,
  score_value INTEGER NOT NULL,
  condition_expression TEXT,
  risk_level TEXT NOT NULL,
  guideline_source TEXT,
  version TEXT,
  is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pre_screening_evaluations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  case_id INTEGER NOT NULL,
  guideline_id INTEGER NOT NULL,
  applied_score INTEGER NOT NULL,
  remarks TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE,
  FOREIGN KEY (guideline_id) REFERENCES pre_screening_guidelines(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS appointments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  patient_id INTEGER NOT NULL,
  clinic_personnel_id INTEGER,
  clinic_id INTEGER NOT NULL,
  appointment_datetime TEXT NOT NULL,
  status TEXT NOT NULL,
  type TEXT NOT NULL,
  case_id INTEGER NOT NULL,
  queue_position INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE RESTRICT,
  FOREIGN KEY (clinic_personnel_id) REFERENCES clinic_personnel(id) ON DELETE SET NULL,
  FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE RESTRICT,
  FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS vaccination_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  case_id INTEGER NOT NULL,
  vaccine_type TEXT NOT NULL,
  dose_number TEXT NOT NULL,
  date_administered TEXT NOT NULL,
  administered_by_personnel_id INTEGER NOT NULL,
  dose_amount TEXT,
  route_site TEXT,
  vaccine_brand_batch TEXT,
  notes TEXT,
  next_dose_date TEXT,
  FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE,
  FOREIGN KEY (administered_by_personnel_id) REFERENCES clinic_personnel(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS case_notes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  case_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  note_content TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS medical_audit_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clinic_personnel_id INTEGER NOT NULL,
  user_id INTEGER,
  entity_type TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  case_id INTEGER,
  action TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE')),
  field_name TEXT,
  old_value TEXT,
  new_value TEXT,
  changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  change_reason TEXT,
  FOREIGN KEY (clinic_personnel_id) REFERENCES clinic_personnel(id) ON DELETE RESTRICT,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
  FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE SET NULL
);

-- =========================
-- System tables
-- =========================

CREATE TABLE IF NOT EXISTS reference_codes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  case_id INTEGER NOT NULL UNIQUE,
  code TEXT NOT NULL UNIQUE,
  generated_by_user_id INTEGER,
  expiration_date TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE,
  FOREIGN KEY (generated_by_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_user_id INTEGER,
  target_role TEXT,
  message_content TEXT NOT NULL,
  notification_type TEXT NOT NULL,
  case_id INTEGER,
  is_sent INTEGER NOT NULL DEFAULT 0 CHECK(is_sent IN (0,1)),
  sent_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (target_user_id) REFERENCES users(id) ON DELETE SET NULL,
  FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clinic_id INTEGER,
  report_type TEXT NOT NULL,
  generation_date TEXT NOT NULL,
  data_link TEXT,
  generated_by_user_id INTEGER NOT NULL,
  FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE SET NULL,
  FOREIGN KEY (generated_by_user_id) REFERENCES users(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS patient_guidance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  guidance_type TEXT NOT NULL,
  related_risk_category TEXT,
  is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
  clinic_id INTEGER NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE RESTRICT
);

-- =========================
-- Required indexes
-- =========================

CREATE INDEX IF NOT EXISTS idx_cases_patient_id ON cases(patient_id);
CREATE INDEX IF NOT EXISTS idx_cases_clinic_id ON cases(clinic_id);

CREATE INDEX IF NOT EXISTS idx_eval_case_id ON pre_screening_evaluations(case_id);
CREATE INDEX IF NOT EXISTS idx_eval_guideline_id ON pre_screening_evaluations(guideline_id);

CREATE INDEX IF NOT EXISTS idx_appt_case_id ON appointments(case_id);
CREATE INDEX IF NOT EXISTS idx_appt_patient_id ON appointments(patient_id);
CREATE INDEX IF NOT EXISTS idx_appt_clinic_id ON appointments(clinic_id);

CREATE INDEX IF NOT EXISTS idx_vax_case_id ON vaccination_records(case_id);

CREATE INDEX IF NOT EXISTS idx_notes_case_id ON case_notes(case_id);

CREATE INDEX IF NOT EXISTS idx_notifications_target_user ON notifications(target_user_id);
CREATE INDEX IF NOT EXISTS idx_notifications_case_id ON notifications(case_id);

CREATE INDEX IF NOT EXISTS idx_reports_clinic_id ON reports(clinic_id);

CREATE INDEX IF NOT EXISTS idx_guidance_clinic_id ON patient_guidance(clinic_id);

-- medical_audit_logs required indexes
CREATE INDEX IF NOT EXISTS idx_audit_personnel ON medical_audit_logs(clinic_personnel_id);
CREATE INDEX IF NOT EXISTS idx_audit_entity ON medical_audit_logs(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_case ON medical_audit_logs(case_id);

-- =========================
-- Additional helpful indexes (FKs / common lookups)
-- =========================

CREATE INDEX IF NOT EXISTS idx_clinic_personnel_clinic_id ON clinic_personnel(clinic_id);

CREATE INDEX IF NOT EXISTS idx_patients_user_id ON patients(user_id);

CREATE INDEX IF NOT EXISTS idx_appointments_personnel_id ON appointments(clinic_personnel_id);

CREATE INDEX IF NOT EXISTS idx_vax_administered_by_personnel_id ON vaccination_records(administered_by_personnel_id);

CREATE INDEX IF NOT EXISTS idx_notes_user_id ON case_notes(user_id);

CREATE INDEX IF NOT EXISTS idx_reference_codes_generated_by_user_id ON reference_codes(generated_by_user_id);

CREATE INDEX IF NOT EXISTS idx_notifications_target_role ON notifications(target_role);

CREATE INDEX IF NOT EXISTS idx_reports_generated_by_user_id ON reports(generated_by_user_id);

COMMIT;

