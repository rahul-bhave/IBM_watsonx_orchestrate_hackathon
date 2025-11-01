"""
Finance Tools for the Finance Agent
These tools interact with the Core Banking System (SQLite database) to manage pension transactions.
"""

from ibm_watsonx_orchestrate.agent_builder.tools import tool
import sqlite3
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import uuid

# Database path - use persistent location for cloud environments
# In IBM Watsonx Orchestrate, we need to use a path that persists across tool invocations
# Try multiple locations in order of preference
def get_db_path():
    """Get the database path, trying multiple locations."""
    # Priority 1: Environment variable
    if os.getenv('FINANCE_DB_PATH'):
        return os.getenv('FINANCE_DB_PATH')
    
    # Priority 2: Current working directory
    try:
        cwd_path = os.path.join(os.getcwd(), 'finance.db')
        # Test if we can write to this location
        test_path = os.path.join(os.getcwd(), '.test_write')
        with open(test_path, 'w') as f:
            f.write('test')
        os.remove(test_path)
        return cwd_path
    except (OSError, PermissionError):
        pass
    
    # Priority 3: /tmp as fallback (ephemeral but at least works)
    return '/tmp/finance.db'

DB_PATH = get_db_path()

def init_database():
    """Initialize database with sample data if it doesn't exist or is empty."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check if customers table exists and has data
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
            account_number TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS pension_details (
            pension_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            pension_type TEXT NOT NULL,
            monthly_amount REAL NOT NULL,
            start_date DATE NOT NULL,
            status TEXT DEFAULT 'active',
            bank_account TEXT,
            ifsc_code TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            pension_id TEXT NOT NULL,
            amount REAL NOT NULL,
            transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            transaction_type TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            is_fraudulent INTEGER DEFAULT 0,
            scheduled_date DATE,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (pension_id) REFERENCES pension_details(pension_id)
        );
        
        CREATE TABLE IF NOT EXISTS fraud_indicators (
            indicator_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT NOT NULL,
            indicator_type TEXT NOT NULL,
            description TEXT,
            severity TEXT DEFAULT 'low',
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        
        CREATE TABLE IF NOT EXISTS scheduled_deposits (
            schedule_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            pension_id TEXT NOT NULL,
            amount REAL NOT NULL,
            frequency TEXT NOT NULL,
            next_deposit_date DATE NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (pension_id) REFERENCES pension_details(pension_id)
        );
    ''')
    
    # Insert sample data (use INSERT OR IGNORE to avoid duplicates)
    customers_data = [
        ('C001', 'John Doe', 'ACC1001', 'john.doe@email.com', '+1-555-0101'),
        ('C002', 'Jane Smith', 'ACC1002', 'jane.smith@email.com', '+1-555-0102'),
        ('C003', 'Robert Johnson', 'ACC1003', 'robert.j@email.com', '+1-555-0103'),
    ]
    cursor.executemany('INSERT OR IGNORE INTO customers VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)', customers_data)
    
    pension_data = [
        ('P001', 'C001', 'Government Pension', 2500.0, '2020-01-01', 'active', 'ACC1001', 'BANK001'),
        ('P002', 'C002', 'Private Pension', 3200.0, '2019-06-15', 'active', 'ACC1002', 'BANK002'),
        ('P003', 'C003', 'Government Pension', 2800.0, '2021-03-20', 'active', 'ACC1003', 'BANK003'),
    ]
    cursor.executemany('INSERT OR IGNORE INTO pension_details VALUES (?, ?, ?, ?, ?, ?, ?, ?)', pension_data)

    
    conn.commit()
    conn.close()


def get_db_connection():
    """Get a connection to the SQLite database."""
    init_database()  # Ensure database exists
    return sqlite3.connect(DB_PATH)


@tool
def initializeDatabase() -> str:
    """Manually reinitialize the database with sample customer data.
    
    This tool resets the database and creates 3 sample customers:
    - C001 (John Doe): $2,500/month Government Pension
    - C002 (Jane Smith): $3,200/month Private Pension
    - C003 (Robert Johnson): $2,800/month Government Pension
    
    Returns:
        str: JSON string confirming database initialization status.
    """
    import traceback
    
    try:
        # Get debug information
        debug_info = {
            "db_path": DB_PATH,
            "db_exists_before": os.path.exists(DB_PATH),
            "current_directory": os.getcwd(),
            "can_write": os.access(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else '.', os.W_OK)
        }
        
        # Force reinitialization by deleting and recreating
        if os.path.exists(DB_PATH):
            try:
                os.remove(DB_PATH)
                debug_info["deleted_old_db"] = True
            except Exception as e:
                debug_info["delete_error"] = str(e)
        
        # Initialize database
        init_database()
        debug_info["init_completed"] = True
        debug_info["db_exists_after"] = os.path.exists(DB_PATH)
        
        # Verify customers exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT customer_id, name FROM customers")
        customers = cursor.fetchall()
        cursor.execute("SELECT pension_id, customer_id, monthly_amount FROM pension_details")
        pensions = cursor.fetchall()
        conn.close()
        
        return json.dumps({
            "success": True,
            "message": "Database initialized successfully! You can now setup pension deposits for customers C001, C002, or C003.",
            "customers": [{"id": c[0], "name": c[1]} for c in customers],
            "pensions": [{"id": p[0], "customer_id": p[1], "amount": p[2]} for p in pensions],
            "debug_info": debug_info
        }, indent=2)
    except Exception as e:
        error_details = {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "db_path": DB_PATH,
            "current_directory": os.getcwd(),
            "db_exists": os.path.exists(DB_PATH) if 'DB_PATH' in dir() else "Path not defined"
        }
        
        # Try to get more context
        try:
            error_details["directory_writable"] = os.access(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else '.', os.W_OK)
            error_details["directory_contents"] = os.listdir(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else '.')[:10]
        except:
            pass
        
        return json.dumps(error_details, indent=2)


@tool
def getPensionDetails(customer_id: str) -> str:
    """Retrieves pension details from the Core Banking System for a specific customer.
    
    Gets complete pension information including account details, pension type, monthly amount,
    and recent transaction history from the Core Banking System database.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001, C002).
        
    Returns:
        str: JSON string containing pension details with customer name, account number, 
            pension type, monthly amount, bank account, and recent transactions.
    """
    return json.dumps(_get_pension_details_internal(customer_id), indent=2)


def _get_pension_details_internal(customer_id: str) -> Dict[str, Any]:
    """
    Internal function: Pull pension details from the Core Banking System for a given customer.
    
    Args:
        customer_id: The unique identifier for the customer
        
    Returns:
        Dictionary containing pension details including amount, type, and account info
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get customer information
        cursor.execute('''
            SELECT customer_id, name, account_number, email, phone
            FROM customers
            WHERE customer_id = ?
        ''', (customer_id,))
        
        customer = cursor.fetchone()
        
        if not customer:
            return {
                "success": False,
                "error": f"Customer {customer_id} not found in the system",
                "customer_id": customer_id
            }
        
        # Get pension details
        cursor.execute('''
            SELECT pension_id, pension_type, monthly_amount, start_date, 
                   status, bank_account, ifsc_code
            FROM pension_details
            WHERE customer_id = ? AND status = 'active'
        ''', (customer_id,))
        
        pension = cursor.fetchone()
        
        if not pension:
            return {
                "success": False,
                "error": f"No active pension found for customer {customer_id}",
                "customer_id": customer_id,
                "customer_name": customer[1]
            }
        
        # Get recent transaction history
        cursor.execute('''
            SELECT transaction_id, amount, transaction_date, status, is_fraudulent
            FROM transactions
            WHERE customer_id = ?
            ORDER BY transaction_date DESC
            LIMIT 5
        ''', (customer_id,))
        
        recent_transactions = cursor.fetchall()
        
        return {
            "success": True,
            "customer_id": customer[0],
            "customer_name": customer[1],
            "account_number": customer[2],
            "email": customer[3],
            "phone": customer[4],
            "pension_id": pension[0],
            "pension_type": pension[1],
            "monthly_amount": pension[2],
            "start_date": pension[3],
            "status": pension[4],
            "bank_account": pension[5],
            "ifsc_code": pension[6],
            "recent_transactions": [
                {
                    "transaction_id": t[0],
                    "amount": t[1],
                    "date": t[2],
                    "status": t[3],
                    "is_fraudulent": bool(t[4])
                }
                for t in recent_transactions
            ]
        }
    
    finally:
        conn.close()


@tool
def checkFraudStatus(customer_id: str, pension_id: str, amount: float) -> str:
    """Checks if a pension transaction is fraudulent or suspicious.
    
    Uses a 5-level fraud detection system including amount deviation check, low-value fraud 
    detection, velocity analysis, duplicate transaction detection, and historical fraud pattern matching.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001).
        pension_id (str): The pension account identifier (e.g., P001).
        amount (float): The transaction amount to verify (e.g., 800.50).
        
    Returns:
        str: JSON string containing fraud check results with is_fraudulent flag, severity level,
            fraud indicators list, fraud score, and recommendation (APPROVE/REJECT).
    """
    return json.dumps(_check_fraud_internal(customer_id, pension_id, amount), indent=2)


def _check_fraud_internal(customer_id: str, pension_id: str, amount: float) -> Dict[str, Any]:
    """
    Internal function: Check if a transaction is fraudulent or suspicious based on multiple indicators.
    
    Args:
        customer_id: The unique identifier for the customer
        pension_id: The pension account identifier
        amount: The transaction amount to check
        
    Returns:
        Dictionary containing fraud check results with is_fraudulent flag and reasons
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    fraud_indicators = []
    is_fraudulent = False
    severity = "low"
    
    try:
        # Get the expected pension amount
        cursor.execute('''
            SELECT monthly_amount
            FROM pension_details
            WHERE pension_id = ? AND customer_id = ?
        ''', (pension_id, customer_id))
        
        pension_data = cursor.fetchone()
        
        if not pension_data:
            return {
                "success": False,
                "error": "Pension details not found",
                "is_fraudulent": True,
                "fraud_indicators": [{"type": "invalid_account", "description": "Invalid pension account", "severity": "high"}],
                "fraud_score": 1,
                "recommendation": "REJECT",
                "severity": "high"
            }
        
        expected_amount = pension_data[0]
        
        # Check 1: Amount validation - Unusual amount (more than 50% deviation)
        amount_deviation = abs(amount - expected_amount) / expected_amount
        if amount_deviation > 0.5:
            is_fraudulent = True
            severity = "high"
            fraud_indicators.append({
                "type": "unusual_amount",
                "description": f"Amount ${amount:.2f} deviates {amount_deviation*100:.1f}% from expected ${expected_amount:.2f}",
                "severity": "high"
            })
        elif amount_deviation > 0.2:
            fraud_indicators.append({
                "type": "unusual_amount",
                "description": f"Amount ${amount:.2f} slightly deviates from expected ${expected_amount:.2f}",
                "severity": "medium"
            })
        
        # Check 2: Suspiciously low amounts
        if amount < 100:
            is_fraudulent = True
            severity = "high" if severity != "high" else severity
            fraud_indicators.append({
                "type": "suspicious_low_amount",
                "description": f"Amount ${amount:.2f} is suspiciously low for pension deposit",
                "severity": "high"
            })
        
        # Check 3: Velocity check - Multiple transactions in last 24 hours
        # Changed from >= 2 to > 3 to allow for legitimate repeat attempts
        yesterday = (datetime.now() - timedelta(days=1)).isoformat()
        cursor.execute('''
            SELECT COUNT(*)
            FROM transactions
            WHERE customer_id = ? AND transaction_date > ?
        ''', (customer_id, yesterday))
        
        recent_count = cursor.fetchone()[0]
        
        if recent_count > 3:
            is_fraudulent = True
            severity = "high"
            fraud_indicators.append({
                "type": "velocity_check",
                "description": f"{recent_count} transactions detected in last 24 hours - possible fraud",
                "severity": "high"
            })
        
        # Check 4: Check for RECENT critical fraud indicators (last 7 days only)
        # Only consider recent high-severity indicators that should block transactions
        seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
        cursor.execute('''
            SELECT indicator_type, description, severity
            FROM fraud_indicators
            WHERE customer_id = ? 
              AND detected_at > ?
              AND severity = 'high'
            ORDER BY detected_at DESC
            LIMIT 3
        ''', (customer_id, seven_days_ago))
        
        existing_indicators = cursor.fetchall()
        
        # Only flag as fraud if there are multiple recent high-severity indicators
        if len(existing_indicators) >= 2:
            is_fraudulent = True
            severity = "high"
            for indicator in existing_indicators:
                fraud_indicators.append({
                    "type": indicator[0],
                    "description": indicator[1],
                    "severity": indicator[2]
                })
        
        # Check 5: Duplicate schedule check (check scheduled_deposits not transactions)
        cursor.execute('''
            SELECT schedule_id, amount, next_deposit_date, status
            FROM scheduled_deposits
            WHERE customer_id = ? AND pension_id = ? AND status = 'active'
        ''', (customer_id, pension_id))
        
        existing_schedule = cursor.fetchone()
        
        if existing_schedule:
            # Don't mark as fraud if there's already an active schedule - this is normal behavior
            # The scheduleRecurringDeposit function will handle updating it
            pass
        
        result = {
            "success": True,
            "customer_id": customer_id,
            "pension_id": pension_id,
            "amount": amount,
            "expected_amount": expected_amount,
            "is_fraudulent": is_fraudulent,
            "severity": severity,
            "fraud_indicators": fraud_indicators,
            "fraud_score": len(fraud_indicators),
            "recommendation": "REJECT" if is_fraudulent else "APPROVE"
        }
        
        # Log the fraud check if suspicious
        if is_fraudulent:
            for indicator in fraud_indicators:
                cursor.execute('''
                    INSERT INTO fraud_indicators (customer_id, indicator_type, description, severity)
                    VALUES (?, ?, ?, ?)
                ''', (customer_id, indicator["type"], indicator["description"], indicator["severity"]))
            conn.commit()
        
        return result
    
    finally:
        conn.close()


@tool
def scheduleRecurringDeposit(
    customer_id: str, 
    pension_id: str = None, 
    amount: float = None,
    frequency: str = "monthly"
) -> str:
    """Schedules or updates a recurring pension deposit for a customer.
    
    Automatically retrieves pension_id and standard amount if not provided.
    Performs fraud verification before creating deposit schedules.
    
    Args:
        customer_id (str): The unique identifier for the customer (e.g., C001). REQUIRED.
        pension_id (str): The pension account identifier (e.g., P001). OPTIONAL - auto-retrieved if not provided.
        amount (float): The deposit amount (e.g., 800.00). OPTIONAL - uses standard pension amount if not provided.
        frequency (str): Deposit frequency - "monthly" (default) or "weekly".
        
    Returns:
        str: JSON string containing schedule confirmation with schedule_id, transaction_id,
            next_deposit_date, and success message.
    """
    return json.dumps(_schedule_recurring_deposit_internal(customer_id, pension_id, amount, frequency), indent=2)


def _schedule_recurring_deposit_internal(
    customer_id: str, 
    pension_id: str = None, 
    amount: float = None,
    frequency: str = "monthly"
) -> Dict[str, Any]:
    """
    Internal function: Schedule a recurring pension deposit if the transaction is not fraudulent.
    
    Args:
        customer_id: The unique identifier for the customer
        pension_id: The pension account identifier (auto-retrieved if None)
        amount: The monthly deposit amount (uses standard amount if None)
        frequency: Deposit frequency (default: "monthly")
        
    Returns:
        Dictionary containing schedule confirmation details
    """
    # Auto-retrieve pension_id and amount if not provided
    if pension_id is None or amount is None:
        pension_details = _get_pension_details_internal(customer_id)
        if not pension_details.get('success'):
            return pension_details  # Return error if customer not found
        
        if pension_id is None:
            pension_id = pension_details['pension_id']
        if amount is None:
            amount = pension_details['monthly_amount']
    
    # STEP 1: Perform fraud check BEFORE scheduling
    fraud_check = _check_fraud_internal(customer_id, pension_id, amount)
    
    # Check if fraud_check has the expected structure
    if not isinstance(fraud_check, dict) or 'is_fraudulent' not in fraud_check:
        return {
            "success": False,
            "error": "Fraud check failed to execute properly",
            "customer_id": customer_id,
            "pension_id": pension_id,
            "amount": amount
        }
    
    if fraud_check.get("is_fraudulent", False):
        return {
            "success": False,
            "error": "Transaction rejected due to fraud detection",
            "customer_id": customer_id,
            "pension_id": pension_id,
            "amount": amount,
            "fraud_check": fraud_check,
            "message": f"FRAUD DETECTED: {fraud_check.get('recommendation', 'REJECT')} - Found {fraud_check.get('fraud_score', 0)} fraud indicators"
        }
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Verify the customer and pension exist
        cursor.execute('''
            SELECT c.name, p.pension_type, p.bank_account
            FROM customers c
            JOIN pension_details p ON c.customer_id = p.customer_id
            WHERE c.customer_id = ? AND p.pension_id = ?
        ''', (customer_id, pension_id))
        
        result = cursor.fetchone()
        
        if not result:
            return {
                "success": False,
                "error": "Customer or pension account not found",
                "customer_id": customer_id,
                "pension_id": pension_id
            }
        
        customer_name, pension_type, bank_account = result
        
        # Generate unique schedule ID
        schedule_id = f"S{str(uuid.uuid4())[:8].upper()}"
        
        # Calculate next deposit date (30 days from now for monthly)
        if frequency == "monthly":
            next_deposit = (datetime.now() + timedelta(days=30)).date()
        elif frequency == "weekly":
            next_deposit = (datetime.now() + timedelta(days=7)).date()
        else:
            next_deposit = (datetime.now() + timedelta(days=30)).date()
        
        # Check if a schedule already exists
        cursor.execute('''
            SELECT schedule_id, status
            FROM scheduled_deposits
            WHERE customer_id = ? AND pension_id = ? AND status = 'active'
        ''', (customer_id, pension_id))
        
        existing_schedule = cursor.fetchone()
        
        if existing_schedule:
            # Update existing schedule
            cursor.execute('''
                UPDATE scheduled_deposits
                SET amount = ?, frequency = ?, next_deposit_date = ?
                WHERE schedule_id = ?
            ''', (amount, frequency, next_deposit.isoformat(), existing_schedule[0]))
            
            schedule_id = existing_schedule[0]
            action = "updated"
        else:
            # Create new schedule
            cursor.execute('''
                INSERT INTO scheduled_deposits 
                (schedule_id, customer_id, pension_id, amount, frequency, next_deposit_date, status)
                VALUES (?, ?, ?, ?, ?, ?, 'active')
            ''', (schedule_id, customer_id, pension_id, amount, frequency, next_deposit.isoformat()))
            
            action = "created"
        
        # Create a transaction record for the scheduled deposit
        transaction_id = f"T{str(uuid.uuid4())[:8].upper()}"
        cursor.execute('''
            INSERT INTO transactions 
            (transaction_id, customer_id, pension_id, amount, transaction_date, 
             transaction_type, status, is_fraudulent, scheduled_date)
            VALUES (?, ?, ?, ?, ?, 'deposit', 'scheduled', 0, ?)
        ''', (transaction_id, customer_id, pension_id, amount, 
              datetime.now().isoformat(), next_deposit.isoformat()))
        
        conn.commit()
        
        return {
            "success": True,
            "action": action,
            "schedule_id": schedule_id,
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "customer_name": customer_name,
            "pension_id": pension_id,
            "pension_type": pension_type,
            "bank_account": bank_account,
            "amount": amount,
            "frequency": frequency,
            "next_deposit_date": next_deposit.isoformat(),
            "status": "active",
            "message": f"Successfully {action} {frequency} pension deposit schedule for {customer_name}"
        }
    
    except Exception as e:
        conn.rollback()
        return {
            "success": False,
            "error": str(e),
            "customer_id": customer_id,
            "pension_id": pension_id
        }
    
    finally:
        conn.close()


@tool
def getScheduledDeposits(customer_id: str = None) -> str:
    """Retrieves all scheduled deposits from the system.
    
    Shows schedule details including amount, frequency, next deposit date, and status.
    Can be filtered by a specific customer or return all schedules.
    
    Args:
        customer_id (str, optional): Optional customer ID to filter results (e.g., C001).
            If not provided, returns all scheduled deposits.
        
    Returns:
        str: JSON string containing list of scheduled deposits with schedule_id, customer_name,
            pension_id, amount, frequency, next_deposit_date, and status for each schedule.
    """
    return json.dumps(_get_scheduled_deposits_internal(customer_id), indent=2)


def _get_scheduled_deposits_internal(customer_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Internal function: Get all scheduled deposits, optionally filtered by customer.
    
    Args:
        customer_id: Optional customer ID to filter by
        
    Returns:
        Dictionary containing list of scheduled deposits
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if customer_id:
            cursor.execute('''
                SELECT s.schedule_id, s.customer_id, c.name, s.pension_id, 
                       s.amount, s.frequency, s.next_deposit_date, s.status
                FROM scheduled_deposits s
                JOIN customers c ON s.customer_id = c.customer_id
                WHERE s.customer_id = ?
                ORDER BY s.next_deposit_date
            ''', (customer_id,))
        else:
            cursor.execute('''
                SELECT s.schedule_id, s.customer_id, c.name, s.pension_id, 
                       s.amount, s.frequency, s.next_deposit_date, s.status
                FROM scheduled_deposits s
                JOIN customers c ON s.customer_id = c.customer_id
                ORDER BY s.next_deposit_date
            ''')
        
        schedules = cursor.fetchall()
        
        return {
            "success": True,
            "count": len(schedules),
            "schedules": [
                {
                    "schedule_id": s[0],
                    "customer_id": s[1],
                    "customer_name": s[2],
                    "pension_id": s[3],
                    "amount": s[4],
                    "frequency": s[5],
                    "next_deposit_date": s[6],
                    "status": s[7]
                }
                for s in schedules
            ]
        }
    
    finally:
        conn.close()


# Backward compatibility - keep old function names that reference new ones
def get_pension_details(customer_id: str) -> Dict[str, Any]:
    """Backward compatibility wrapper."""
    return _get_pension_details_internal(customer_id)


def check_fraud(customer_id: str, pension_id: str, amount: float) -> Dict[str, Any]:
    """Backward compatibility wrapper."""
    return _check_fraud_internal(customer_id, pension_id, amount)


def schedule_recurring_deposit(customer_id: str, pension_id: str, amount: float, frequency: str = "monthly") -> Dict[str, Any]:
    """Backward compatibility wrapper."""
    return _schedule_recurring_deposit_internal(customer_id, pension_id, amount, frequency)


def get_scheduled_deposits(customer_id: Optional[str] = None) -> Dict[str, Any]:
    """Backward compatibility wrapper."""
    return _get_scheduled_deposits_internal(customer_id)


# Tool definitions for IBM Watsonx Orchestrate
TOOL_DEFINITIONS = {
    "get_pension_details": {
        "name": "get_pension_details",
        "description": "Retrieves pension details from the Core Banking System for a specific customer",
        "parameters": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The unique identifier for the customer (e.g., C001)"
                }
            },
            "required": ["customer_id"]
        }
    },
    "check_fraud": {
        "name": "check_fraud",
        "description": "Checks if a pension transaction is fraudulent or suspicious based on multiple indicators",
        "parameters": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The unique identifier for the customer"
                },
                "pension_id": {
                    "type": "string",
                    "description": "The pension account identifier"
                },
                "amount": {
                    "type": "number",
                    "description": "The transaction amount to verify"
                }
            },
            "required": ["customer_id", "pension_id", "amount"]
        }
    },
    "schedule_recurring_deposit": {
        "name": "schedule_recurring_deposit",
        "description": "Schedules a recurring pension deposit for a customer",
        "parameters": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The unique identifier for the customer"
                },
                "pension_id": {
                    "type": "string",
                    "description": "The pension account identifier"
                },
                "amount": {
                    "type": "number",
                    "description": "The monthly deposit amount"
                },
                "frequency": {
                    "type": "string",
                    "description": "Deposit frequency (monthly, weekly, etc.)",
                    "default": "monthly"
                }
            },
            "required": ["customer_id", "pension_id", "amount"]
        }
    }
}
