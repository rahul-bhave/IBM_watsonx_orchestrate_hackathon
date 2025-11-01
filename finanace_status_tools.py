"""
Finance Status Tools for Finthropy Finance Agent
These tools provide comprehensive financial status tracking including net worth, spending, bills, and income.
"""

from ibm_watsonx_orchestrate.agent_builder.tools import tool
import sqlite3
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# Database path - use persistent location for cloud environments
# In IBM Watsonx Orchestrate, we need to use a path that persists across tool invocations
# Try multiple locations in order of preference
def get_db_path():
    """Get the database path, trying multiple locations."""
    # Priority 1: Environment variable
    if os.getenv('FINANCE_STATUS_DB_PATH'):
        return os.getenv('FINANCE_STATUS_DB_PATH')
    
    # Priority 2: Current working directory
    try:
        cwd_path = os.path.join(os.getcwd(), 'finance_status.db')
        # Test if we can write to this location
        test_path = os.path.join(os.getcwd(), '.test_write')
        with open(test_path, 'w') as f:
            f.write('test')
        os.remove(test_path)
        return cwd_path
    except (OSError, PermissionError):
        pass
    
    # Priority 3: /tmp as fallback (ephemeral but at least works)
    return '/tmp/finance_status.db'

DB_PATH = get_db_path()

def init_database():
    """Initialize database with comprehensive financial data if it doesn't exist or is empty."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check if tables exist and have data
        cursor.execute("SELECT COUNT(*) FROM customers")
        count = cursor.fetchone()[0]
        if count > 0:
            return  # Database already has data
    except sqlite3.OperationalError:
        # Tables don't exist yet
        pass
    
    # Create tables
    cursor.executescript('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT,
            phone TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS financial_status (
            status_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            net_worth REAL NOT NULL,
            current_balance REAL NOT NULL,
            emergency_fund REAL NOT NULL,
            emergency_target REAL NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        
        CREATE TABLE IF NOT EXISTS spending (
            spending_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT NOT NULL,
            category TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            spending_date DATE NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        
        CREATE TABLE IF NOT EXISTS bills (
            bill_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT NOT NULL,
            bill_name TEXT NOT NULL,
            amount REAL NOT NULL,
            due_date DATE NOT NULL,
            status TEXT DEFAULT 'pending',
            category TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        
        CREATE TABLE IF NOT EXISTS income (
            income_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT NOT NULL,
            income_type TEXT NOT NULL,
            amount REAL NOT NULL,
            expected_date DATE NOT NULL,
            actual_date DATE,
            status TEXT DEFAULT 'pending',
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        
        CREATE TABLE IF NOT EXISTS tax_tips (
            tip_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT NOT NULL,
            tip_category TEXT NOT NULL,
            suggestion TEXT NOT NULL,
            estimated_savings REAL,
            applicable_period TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
    ''')
    
    # Insert sample customers
    customers_data = [
        ('C001', 'John Smith', 67, 'john.smith@email.com', '+1-555-0101'),
        ('C002', 'Mary Johnson', 72, 'mary.j@email.com', '+1-555-0102'),
        ('C003', 'Robert Williams', 69, 'robert.w@email.com', '+1-555-0103'),
    ]
    cursor.executemany('INSERT OR IGNORE INTO customers (customer_id, name, age, email, phone) VALUES (?, ?, ?, ?, ?)', customers_data)
    
    # Insert financial status
    current_date = datetime.now()
    financial_status_data = [
        ('FS001', 'C001', 85000.00, 4500.00, 500.00, 1000.00, current_date.isoformat()),
        ('FS002', 'C002', 92000.00, 5200.00, 1200.00, 1500.00, current_date.isoformat()),
        ('FS003', 'C003', 78000.00, 3800.00, 800.00, 1200.00, current_date.isoformat()),
    ]
    cursor.executemany('INSERT OR IGNORE INTO financial_status VALUES (?, ?, ?, ?, ?, ?, ?)', financial_status_data)
    
    # Insert spending for current month
    spending_data = [
        ('C001', 'Groceries', 450.00, 'Weekly grocery shopping', (current_date - timedelta(days=5)).date().isoformat()),
        ('C001', 'Healthcare', 320.00, 'Prescription medications', (current_date - timedelta(days=10)).date().isoformat()),
        ('C001', 'Utilities', 180.00, 'Water and electricity', (current_date - timedelta(days=15)).date().isoformat()),
        ('C001', 'Entertainment', 150.00, 'Movies and dining', (current_date - timedelta(days=7)).date().isoformat()),
        ('C001', 'Transportation', 100.00, 'Gas and maintenance', (current_date - timedelta(days=3)).date().isoformat()),
        ('C002', 'Groceries', 520.00, 'Grocery shopping', (current_date - timedelta(days=6)).date().isoformat()),
        ('C002', 'Healthcare', 410.00, 'Doctor visit and meds', (current_date - timedelta(days=12)).date().isoformat()),
        ('C002', 'Entertainment', 200.00, 'Concert tickets', (current_date - timedelta(days=8)).date().isoformat()),
        ('C003', 'Groceries', 380.00, 'Weekly shopping', (current_date - timedelta(days=4)).date().isoformat()),
        ('C003', 'Utilities', 220.00, 'Internet and phone', (current_date - timedelta(days=14)).date().isoformat()),
    ]
    cursor.executemany('INSERT INTO spending (customer_id, category, amount, description, spending_date) VALUES (?, ?, ?, ?, ?)', spending_data)
    
    # Insert upcoming bills
    bills_data = [
        ('C001', 'Electricity', 120.00, (current_date + timedelta(days=17)).date().isoformat(), 'pending', 'Utilities'),
        ('C001', 'Insurance', 250.00, (current_date + timedelta(days=22)).date().isoformat(), 'pending', 'Insurance'),
        ('C001', 'Healthcare', 300.00, (current_date + timedelta(days=27)).date().isoformat(), 'pending', 'Healthcare'),
        ('C001', 'Internet', 80.00, (current_date + timedelta(days=12)).date().isoformat(), 'pending', 'Utilities'),
        ('C002', 'Property Tax', 450.00, (current_date + timedelta(days=25)).date().isoformat(), 'pending', 'Tax'),
        ('C002', 'Car Insurance', 320.00, (current_date + timedelta(days=18)).date().isoformat(), 'pending', 'Insurance'),
        ('C002', 'Phone Bill', 95.00, (current_date + timedelta(days=10)).date().isoformat(), 'pending', 'Utilities'),
        ('C003', 'Rent', 800.00, (current_date + timedelta(days=5)).date().isoformat(), 'pending', 'Housing'),
        ('C003', 'Health Insurance', 280.00, (current_date + timedelta(days=20)).date().isoformat(), 'pending', 'Insurance'),
    ]
    cursor.executemany('INSERT INTO bills (customer_id, bill_name, amount, due_date, status, category) VALUES (?, ?, ?, ?, ?, ?)', bills_data)
    
    # Insert income sources
    income_data = [
        ('C001', 'Pension', 1200.00, (current_date + timedelta(days=5)).date().isoformat(), None, 'pending'),
        ('C001', 'Social Security', 900.00, (current_date + timedelta(days=12)).date().isoformat(), None, 'pending'),
        ('C001', 'Rental Income', 400.00, (current_date + timedelta(days=8)).date().isoformat(), None, 'delayed'),
        ('C002', 'Pension', 1500.00, (current_date + timedelta(days=6)).date().isoformat(), None, 'pending'),
        ('C002', 'Social Security', 1100.00, (current_date + timedelta(days=11)).date().isoformat(), None, 'pending'),
        ('C002', 'Investment Income', 300.00, (current_date + timedelta(days=15)).date().isoformat(), None, 'pending'),
        ('C003', 'Pension', 950.00, (current_date + timedelta(days=7)).date().isoformat(), None, 'pending'),
        ('C003', 'Social Security', 850.00, (current_date + timedelta(days=13)).date().isoformat(), None, 'pending'),
    ]
    cursor.executemany('INSERT INTO income (customer_id, income_type, amount, expected_date, actual_date, status) VALUES (?, ?, ?, ?, ?, ?)', income_data)
    
    # Insert tax tips
    tax_tips_data = [
        ('C001', 'Retirement', 'Use tax-efficient withdrawals from retirement accounts', 300.00, 'Annual'),
        ('C001', 'Healthcare', 'Maximize medical expense deductions if over 7.5% AGI', 150.00, 'Annual'),
        ('C001', 'Charitable', 'Consider qualified charitable distributions from IRA', 200.00, 'Annual'),
        ('C002', 'Retirement', 'Optimize Roth conversion ladder strategy', 400.00, 'Annual'),
        ('C002', 'Investment', 'Harvest tax losses from investment portfolio', 250.00, 'Annual'),
        ('C003', 'Healthcare', 'Contribute to HSA for tax-free medical expenses', 180.00, 'Annual'),
        ('C003', 'Retirement', 'Delay Social Security for higher benefits', 500.00, 'Annual'),
    ]
    cursor.executemany('INSERT INTO tax_tips (customer_id, tip_category, suggestion, estimated_savings, applicable_period) VALUES (?, ?, ?, ?, ?)', tax_tips_data)
    
    conn.commit()
    conn.close()


def get_db_connection():
    """Get a connection to the SQLite database."""
    init_database()  # Ensure database exists
    return sqlite3.connect(DB_PATH)


@tool
def initializeFinanceDatabase() -> str:
    """Manually reinitialize the financial status database with sample customer data.
    
    This tool resets the database and creates 3 sample customers with comprehensive financial data:
    - C001 (John Smith): Net Worth $85,000, Balance $4,500
    - C002 (Mary Johnson): Net Worth $92,000, Balance $5,200
    - C003 (Robert Williams): Net Worth $78,000, Balance $3,800
    
    Returns:
        str: JSON string confirming database initialization status.
    """
    try:
        # Force reinitialization by deleting and recreating
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        
        init_database()
        
        # Verify data exists
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT customer_id, name FROM customers")
        customers = cursor.fetchall()
        cursor.execute("SELECT status_id, customer_id, net_worth, current_balance FROM financial_status")
        statuses = cursor.fetchall()
        conn.close()
        
        return json.dumps({
            "success": True,
            "message": "Finance database initialized successfully",
            "customers": [{"id": c[0], "name": c[1]} for c in customers],
            "financial_statuses": [{"id": s[0], "customer_id": s[1], "net_worth": s[2], "balance": s[3]} for s in statuses]
        }, indent=2)
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


@tool
def getFinancialStatus(customer_id: str) -> str:
    """Get comprehensive financial status overview for a customer.
    
    Provides complete financial snapshot including net worth, current balance,
    monthly spending, and upcoming bills.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        
    Returns:
        str: JSON string containing financial status with net worth, current balance,
            this month's spending total, and total upcoming bills.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get customer info
        cursor.execute('SELECT name, email FROM customers WHERE customer_id = ?', (customer_id,))
        customer = cursor.fetchone()
        
        if not customer:
            return json.dumps({
                "success": False,
                "error": f"Customer {customer_id} not found"
            }, indent=2)
        
        # Get financial status
        cursor.execute('''
            SELECT net_worth, current_balance, emergency_fund, emergency_target, last_updated
            FROM financial_status
            WHERE customer_id = ?
        ''', (customer_id,))
        status = cursor.fetchone()
        
        if not status:
            return json.dumps({
                "success": False,
                "error": f"Financial status not found for customer {customer_id}"
            }, indent=2)
        
        # Get this month's spending
        first_day_of_month = datetime.now().replace(day=1).date().isoformat()
        cursor.execute('''
            SELECT SUM(amount)
            FROM spending
            WHERE customer_id = ? AND spending_date >= ?
        ''', (customer_id, first_day_of_month))
        monthly_spending = cursor.fetchone()[0] or 0
        
        # Get upcoming bills total
        today = datetime.now().date().isoformat()
        cursor.execute('''
            SELECT SUM(amount)
            FROM bills
            WHERE customer_id = ? AND due_date >= ? AND status = 'pending'
        ''', (customer_id, today))
        upcoming_bills = cursor.fetchone()[0] or 0
        
        return json.dumps({
            "success": True,
            "customer_id": customer_id,
            "customer_name": customer[0],
            "net_worth": status[0],
            "current_balance": status[1],
            "monthly_spending": monthly_spending,
            "upcoming_bills": upcoming_bills,
            "emergency_fund": status[2],
            "emergency_target": status[3],
            "last_updated": status[4]
        }, indent=2)
    
    finally:
        conn.close()


@tool
def checkBillSufficiency(customer_id: str) -> str:
    """Check if customer has sufficient balance to cover upcoming bills.
    
    Compares current balance with total upcoming bills to determine surplus or deficit.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        
    Returns:
        str: JSON string showing current balance, upcoming bills total, and surplus/deficit.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current balance
        cursor.execute('SELECT current_balance FROM financial_status WHERE customer_id = ?', (customer_id,))
        balance_row = cursor.fetchone()
        
        if not balance_row:
            return json.dumps({
                "success": False,
                "error": f"Financial status not found for customer {customer_id}"
            }, indent=2)
        
        current_balance = balance_row[0]
        
        # Get upcoming bills
        today = datetime.now().date().isoformat()
        cursor.execute('''
            SELECT SUM(amount)
            FROM bills
            WHERE customer_id = ? AND due_date >= ? AND status = 'pending'
        ''', (customer_id, today))
        upcoming_bills = cursor.fetchone()[0] or 0
        
        surplus = current_balance - upcoming_bills
        is_sufficient = surplus >= 0
        
        return json.dumps({
            "success": True,
            "customer_id": customer_id,
            "current_balance": current_balance,
            "upcoming_bills": upcoming_bills,
            "surplus": surplus,
            "is_sufficient": is_sufficient,
            "status": "Sufficient" if is_sufficient else "Insufficient",
            "message": f"You have {'enough' if is_sufficient else 'insufficient'} funds to cover your bills"
        }, indent=2)
    
    finally:
        conn.close()


@tool
def getUpcomingBills(customer_id: str) -> str:
    """Get list of upcoming bills for a customer.
    
    Returns all pending bills with amounts and due dates, sorted by due date.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        
    Returns:
        str: JSON string containing list of upcoming bills with bill name, amount, 
            due date, and category.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        today = datetime.now().date().isoformat()
        cursor.execute('''
            SELECT bill_name, amount, due_date, category, status
            FROM bills
            WHERE customer_id = ? AND due_date >= ? AND status = 'pending'
            ORDER BY due_date ASC
        ''', (customer_id, today))
        
        bills = cursor.fetchall()
        
        bills_list = []
        total_amount = 0
        for bill in bills:
            bills_list.append({
                "bill_name": bill[0],
                "amount": bill[1],
                "due_date": bill[2],
                "category": bill[3],
                "status": bill[4]
            })
            total_amount += bill[1]
        
        return json.dumps({
            "success": True,
            "customer_id": customer_id,
            "bills_count": len(bills_list),
            "total_amount": total_amount,
            "bills": bills_list
        }, indent=2)
    
    finally:
        conn.close()


@tool
def getEmergencyFundStatus(customer_id: str) -> str:
    """Get emergency fund status and recommendations.
    
    Shows current emergency fund balance and compares with recommended target.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        
    Returns:
        str: JSON string containing emergency fund amount, recommended target,
            gap to target, and percentage of target achieved.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT emergency_fund, emergency_target
            FROM financial_status
            WHERE customer_id = ?
        ''', (customer_id,))
        
        result = cursor.fetchone()
        
        if not result:
            return json.dumps({
                "success": False,
                "error": f"Financial status not found for customer {customer_id}"
            }, indent=2)
        
        emergency_fund = result[0]
        emergency_target = result[1]
        gap = emergency_target - emergency_fund
        percentage = (emergency_fund / emergency_target * 100) if emergency_target > 0 else 0
        
        status = "Below Target" if emergency_fund < emergency_target else "Target Met"
        
        return json.dumps({
            "success": True,
            "customer_id": customer_id,
            "emergency_fund": emergency_fund,
            "recommended_target": emergency_target,
            "gap_to_target": gap if gap > 0 else 0,
            "percentage_of_target": round(percentage, 2),
            "status": status,
            "recommendation": f"Build emergency fund by ${gap:.2f}" if gap > 0 else "Emergency fund target achieved!"
        }, indent=2)
    
    finally:
        conn.close()


@tool
def getIncomeStatus(customer_id: str) -> str:
    """Get income deposit status for expected income sources.
    
    Shows all expected income sources with amounts, dates, and status (on time/delayed).
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        
    Returns:
        str: JSON string containing list of income sources with type, amount,
            expected date, actual date (if received), and status.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get upcoming/recent income
        thirty_days_ago = (datetime.now() - timedelta(days=30)).date().isoformat()
        thirty_days_ahead = (datetime.now() + timedelta(days=30)).date().isoformat()
        
        cursor.execute('''
            SELECT income_type, amount, expected_date, actual_date, status
            FROM income
            WHERE customer_id = ? AND expected_date BETWEEN ? AND ?
            ORDER BY expected_date ASC
        ''', (customer_id, thirty_days_ago, thirty_days_ahead))
        
        income_sources = cursor.fetchall()
        
        income_list = []
        total_expected = 0
        on_time_count = 0
        delayed_count = 0
        
        for income in income_sources:
            income_type, amount, expected_date, actual_date, status = income
            
            # Determine if delayed
            expected_dt = datetime.strptime(expected_date, '%Y-%m-%d').date()
            is_delayed = status == 'delayed' or (datetime.now().date() > expected_dt and not actual_date)
            
            if is_delayed:
                delayed_count += 1
            else:
                on_time_count += 1
            
            income_list.append({
                "income_type": income_type,
                "amount": amount,
                "expected_date": expected_date,
                "actual_date": actual_date,
                "status": "Delayed" if is_delayed else ("Received" if actual_date else "On Time"),
                "is_delayed": is_delayed
            })
            total_expected += amount
        
        return json.dumps({
            "success": True,
            "customer_id": customer_id,
            "income_sources_count": len(income_list),
            "total_expected": total_expected,
            "on_time_count": on_time_count,
            "delayed_count": delayed_count,
            "income_sources": income_list
        }, indent=2)
    
    finally:
        conn.close()


@tool
def getMonthlyIncome(customer_id: str) -> str:
    """Get received income for current month.
    
    Shows all income received this month with type and amount.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        
    Returns:
        str: JSON string containing list of income received this month.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get this month's income (use expected_date as proxy since we're in current month)
        first_day_of_month = datetime.now().replace(day=1).date().isoformat()
        
        cursor.execute('''
            SELECT income_type, amount, expected_date, status
            FROM income
            WHERE customer_id = ? AND expected_date >= ?
            ORDER BY expected_date ASC
        ''', (customer_id, first_day_of_month))
        
        income_sources = cursor.fetchall()
        
        income_list = []
        total_income = 0
        
        for income in income_sources:
            income_list.append({
                "income_type": income[0],
                "amount": income[1],
                "date": income[2],
                "status": income[3] or "pending"
            })
            total_income += income[1]
        
        return json.dumps({
            "success": True,
            "customer_id": customer_id,
            "month": datetime.now().strftime("%B %Y"),
            "income_count": len(income_list),
            "total_income": total_income,
            "income_sources": income_list
        }, indent=2)
    
    finally:
        conn.close()


@tool
def getTaxTips(customer_id: str) -> str:
    """Get personalized tax optimization tips for customer.
    
    Provides tax-saving suggestions with estimated savings for the year.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        
    Returns:
        str: JSON string containing tax tips with category, suggestion,
            estimated annual savings, and applicable period.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT tip_category, suggestion, estimated_savings, applicable_period
            FROM tax_tips
            WHERE customer_id = ?
            ORDER BY estimated_savings DESC
        ''', (customer_id,))
        
        tips = cursor.fetchall()
        
        tips_list = []
        total_savings = 0
        
        for tip in tips:
            tips_list.append({
                "category": tip[0],
                "suggestion": tip[1],
                "estimated_savings": tip[2],
                "period": tip[3]
            })
            total_savings += tip[2] if tip[2] else 0
        
        return json.dumps({
            "success": True,
            "customer_id": customer_id,
            "tips_count": len(tips_list),
            "total_estimated_savings": total_savings,
            "tax_tips": tips_list
        }, indent=2)
    
    finally:
        conn.close()


@tool
def getMonthlySpending(customer_id: str) -> str:
    """Get detailed breakdown of spending for current month.
    
    Provides spending by category with amounts and transaction details.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        
    Returns:
        str: JSON string containing spending breakdown by category.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        first_day_of_month = datetime.now().replace(day=1).date().isoformat()
        
        # Get spending by category
        cursor.execute('''
            SELECT category, SUM(amount) as total, COUNT(*) as count
            FROM spending
            WHERE customer_id = ? AND spending_date >= ?
            GROUP BY category
            ORDER BY total DESC
        ''', (customer_id, first_day_of_month))
        
        categories = cursor.fetchall()
        
        spending_list = []
        total_spending = 0
        
        for cat in categories:
            spending_list.append({
                "category": cat[0],
                "amount": cat[1],
                "transaction_count": cat[2]
            })
            total_spending += cat[1]
        
        return json.dumps({
            "success": True,
            "customer_id": customer_id,
            "month": datetime.now().strftime("%B %Y"),
            "total_spending": total_spending,
            "categories_count": len(spending_list),
            "spending_by_category": spending_list
        }, indent=2)
    
    finally:
        conn.close()
