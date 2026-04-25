# 🐾 RabiesResQ: Advanced Clinic Management System

[![Version](https://img.shields.io/badge/version-2.1.0-emerald)]()
[![Status](https://img.shields.io/badge/status-active-success)]()
[![Stack](https://img.shields.io/badge/stack-Python_Flask-blue)]()

**RabiesResQ** is a high-performance, context-aware management system designed for rabies clinics. It streamlines the patient intake process, automates risk categorization based on WHO standards, and ensures critical cases receive immediate attention through a modern, uplifting user interface.

---

## ✨ Key Features

### 🏥 For Clinic Staff & Admin
- **Context-Aware Dashboard**: A prioritized view of high-risk cases and upcoming vaccinations.
- **WHO-Compliant Pre-Screening**: Intelligent intake forms that automatically categorize cases (Category I, II, or III).
- **Consolidated Case Monitoring**: Real-time tracking of pending, ongoing, and completed treatments.
- **Vaccination Management**: Automated dose scheduling and "Due Soon" notifications to prevent missed treatments.
- **Export & Reporting**: Generate detailed clinic performance reports in CSV or PDF formats.

### 👤 For Patients
- **Self-Service Intake**: Report bites and exposure incidents directly through a friendly portal.
- **Progress Tracking**: Real-time visualization of vaccination progress (e.g., "1 dose to go").
- **Care Plan Transparency**: Clear view of past and upcoming treatment sessions.

---

## 🎨 Design Philosophy
RabiesResQ is built with a focus on **Visual Excellence** and **Uplifting User Experience**:
- **Modern Aesthetic**: Clean, spacious layouts using a curated Emerald & Slate color palette.
- **Premium Components**: Custom-built notifications, glassmorphic effects, and smooth micro-animations.
- **Accessibility First**: High-contrast typography and semantic HTML structure.
- **Context-Aware UI**: Components that dynamically adapt their tone and urgency based on clinical data.

---

## 🛠️ Technology Stack
- **Backend**: Python 3.9+ with Flask
- **Database**: SQLite with dynamic schema management
- **Frontend**: Vanilla JavaScript (ES6+), HTML5, CSS3 with Tailwind-inspired design tokens
- **Reporting**: xhtml2pdf & pycairo for professional document generation

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9 or higher
- Pip (Python Package Manager)

### Installation
1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd RABIESRESQ
   ```

2. **Set up virtual environment:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # Mac/Linux
   # .venv\Scripts\activate   # Windows
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Configuration:**
   Copy `.env.example` to `.env` and configure your secret keys:
   ```bash
   cp .env.example .env
   ```

5. **Initialize Database:**
   The system will automatically initialize the SQLite database on first run.

6. **Run the Application:**
   ```bash
   python3 app.py
   ```
   Visit `http://localhost:5000` to start.

---

## 🔒 Security
- Role-based Access Control (RBAC) for Admin, Staff, and Patients.
- Secure session management and password hashing.
- Defensive coding patterns to prevent SQL injection and XSS.

---

## 📄 License
This project is proprietary. All rights reserved.

*Developed with ❤️ for a safer community.*
