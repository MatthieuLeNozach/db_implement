# app.py - Updated Flask Integration with Dynamic Customer Loading
import os
import tempfile
import shutil
import json
from functools import wraps
from pathlib import Path
from datetime import datetime
import logging

from flask import (
    Flask, render_template, request, redirect,
    send_file, url_for, flash, session, jsonify
)
from flask_session import Session as FlaskSession
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

# Import Core Components
from src.core.config import Config
from src.core.logging import setup_logging, time_operation
from src.core.exceptions import (
    FileNotFoundError, AuthenticationError, 
    DatabaseConnectionError, ConfigurationError
)

# Import refactored service
from src.services.purchase_order_service import (
    PurchaseOrderService,
    ExtractionRulesLoader,
    DatabaseIntegration
)
from services.database_service import DatabaseService

# -----------------------------------------------------------------------------
# App setup - UTILIZING CENTRALIZED CONFIG
# -----------------------------------------------------------------------------

# 1. Initialize core configuration (Loads .env, creates dirs, validates)
try:
    Config.initialize()
except ConfigurationError as e:
    print(f"FATAL CONFIGURATION ERROR: {e}")
    exit(1)

app = Flask(__name__, static_folder='static', static_url_path='/static')

# 2. Apply centralized Flask configuration
app.config.update(Config.get_flask_config())

# 3. Setup logging with central module, using Config settings
logger = setup_logging(verbose=Config.app.VERBOSE)
logger.info(Config.summary())

# Ensure required directories exist
Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)
Path(app.config["PO_DIRECTORY"]).mkdir(parents=True, exist_ok=True)

FlaskSession(app)

# -----------------------------------------------------------------------------
# Initialize Services and Load Rules
# -----------------------------------------------------------------------------

# Initialize database service
db_service = DatabaseService()
logger.info("🔧 Initializing Purchase Order Service...")

# Load extraction rules from CSV
rules_path = Path(app.config["RULES_CSV_PATH"])
if not rules_path.exists():
    logger.error(f"❌ Rules CSV not found: {rules_path}")
    raise FileNotFoundError(f"Extraction rules not found: {rules_path}")

try:
    rules_config = ExtractionRulesLoader.load_from_csv(rules_path)
    logger.info(f"✅ Service initialized with {len(rules_config)} customer formats")
except Exception as e:
    logger.error(f"❌ Failed to load extraction rules: {e}")
    raise

# Initialize services
po_service = PurchaseOrderService(rules_config=rules_config)
db_integration = DatabaseIntegration(db_service)

# Verify database connection at startup
try:
    with db_service.get_session() as db_session:
        from models.models import Product
        product_count = db_session.query(Product).count()
        logger.info(f"✅ Database connected. Products: {product_count}")
except Exception as e:
    logger.critical(f"❌ Database connection failed: {e}")
    raise DatabaseConnectionError(f"Startup database check failed: {e}")

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

def get_available_customers():
    """
    Get list of available customers from loaded extraction rules.
    Returns a sorted list of customer names.
    """
    try:
        customers = list(rules_config.keys())
        logger.debug(f"📋 Available customers: {customers}")
        return sorted(customers)
    except Exception as e:
        logger.error(f"❌ Failed to get available customers: {e}")
        return []


def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed (PDF only)."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in Config.app.ALLOWED_EXTENSIONS


def login_required(f):
    """Decorator to require login for routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not Config.app.AUTH_ENABLED or session.get("user"):
            return f(*args, **kwargs)
        return redirect(url_for("login"))
    return decorated_function

# -----------------------------------------------------------------------------
# Auth Routes
# -----------------------------------------------------------------------------

BASIC_AUTH_USERS = {
    "admin": {"password_hash": generate_password_hash(Config.app.DEFAULT_USER_PWD)}
}

@app.route("/ping")
def ping():
    """Health check endpoint."""
    return jsonify({"status": "pong", "env": Config.app.ENV})


@app.route("/health")
def health_check():
    """
    Health check endpoint to verify rules are loaded and services are ready.
    """
    try:
        customers = get_available_customers()
        return jsonify({
            'status': 'healthy',
            'customers_loaded': len(customers),
            'customers': customers,
            'database_connected': True,
            'environment': Config.app.ENV
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route("/login", methods=["GET"])
def login():
    """Display login page."""
    if session.get("user"):
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/login", methods=["POST"])
def login_basic():
    """Handle login form submission."""
    username = request.form.get("username")
    password = request.form.get("password")

    if not username or not password:
        flash("Please enter both username and password", "warning")
        return redirect(url_for("login"))

    user = BASIC_AUTH_USERS.get(username)
    if user and check_password_hash(user["password_hash"], password):
        session["user"] = {"name": username, "auth_type": "basic"}
        flash(f"✅ Logged in as {username}", "success")
        logger.info(f"User {username} logged in")
        return redirect(url_for("index"))

    flash("❌ Invalid credentials", "error")
    logger.warning(f"Failed login attempt for username: {username}")
    return redirect(url_for("login"))


@app.route("/logout")
def logout():
    """Handle logout."""
    username = session.get("user", {}).get("name", "unknown")
    session.clear()
    flash("✅ Logged out successfully", "success")
    logger.info(f"User {username} logged out")
    return redirect(url_for("login"))

# -----------------------------------------------------------------------------
# Main Dashboard
# -----------------------------------------------------------------------------

@app.route("/")
@login_required
def index():
    """
    Main page - PDF extraction interface with dynamic customer list.
    """
    try:
        # Get available customers from loaded rules
        available_customers = get_available_customers()
        
        # Get results from session if they exist
        results = session.get('results', None)
        
        return render_template(
            'index.html',
            user=session.get("user"),
            auth_enabled=Config.app.AUTH_ENABLED,
            available_customers=available_customers,
            results=results,
            batch_results=session.get("batch_results")
        )
    except Exception as e:
        logger.error(f"❌ Error loading index page: {e}", exc_info=True)
        flash(f"Error loading page: {str(e)}", "error")
        return render_template(
            'index.html',
            user=session.get("user"),
            auth_enabled=Config.app.AUTH_ENABLED,
            available_customers=[],
            results=None
        )


@app.route("/clear_results", methods=["POST"])
@login_required
def clear_results():
    """Clear session results."""
    session.pop("results", None)
    session.pop("batch_results", None)
    flash("🗑️ Results cleared", "info")
    logger.info("Session results cleared")
    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# Single File Upload & Processing
# -----------------------------------------------------------------------------

@app.route("/upload_file", methods=["POST"])
@login_required
def upload_file():
    """Handle single file upload and process purchase order PDF."""
    try:
        # Validate file upload
        if "pdf" not in request.files:
            flash("⚠️ No file uploaded", "warning")
            return redirect(url_for("index"))

        file = request.files["pdf"]
        if file.filename == "":
            flash("⚠️ No file selected", "warning")
            return redirect(url_for("index"))

        if not allowed_file(file.filename):
            flash(f"❌ Only {', '.join(Config.app.ALLOWED_EXTENSIONS)} files are allowed", "error")
            return redirect(url_for("index"))

        # Validate customer selection
        customer = request.form.get("customer")
        if not customer:
            flash("⚠️ Please select a customer", "warning")
            return redirect(url_for("index"))

        # Validate customer exists in rules
        available_customers = get_available_customers()
        if customer not in available_customers:
            flash(f"❌ Invalid customer: {customer}", "error")
            logger.error(f"Invalid customer selected: {customer}")
            return redirect(url_for("index"))

        # Save to temporary file
        temp_dir = tempfile.mkdtemp()
        secure_name = secure_filename(file.filename)
        file_path = Path(temp_dir) / secure_name

        try:
            file.save(file_path)
            logger.info(f"📁 Processing uploaded file: {secure_name} for customer: {customer}")

            # Process with timing
            with time_operation(f"Processing {secure_name}", logger=logger) as timer:
                result = po_service.process_file(file_path, customer)

            if result.success:
                # Store results in session for display
                session['results'] = {
                    'customer': customer,
                    'filename': secure_name,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'summary': result.to_dict()
                }

                # Optionally save to database
                save_to_db = request.form.get("save_to_db") == "on"
                if save_to_db:
                    db_result = db_integration.save_result(result)
                    if db_result["saved"]:
                        flash(f"✅ Successfully processed and saved to database: {secure_name}", "success")
                        logger.info(f"Result saved to database for {secure_name}")
                    else:
                        flash(f"⚠️ Processed but database save failed: {db_result.get('error', 'Unknown error')}", "warning")
                        logger.warning(f"Database save failed: {db_result.get('error')}")
                else:
                    flash(f"✅ Successfully processed: {secure_name} in {timer.elapsed_time:.2f}s", "success")

                logger.info(f"✅ File processed: {len(result.lines)} lines extracted")
            else:
                flash(f"❌ Processing failed: {result.error_message}", "error")
                logger.error(f"Processing failed for {secure_name}: {result.error_message}")

        finally:
            # Clean up temporary file
            if file_path.exists():
                file_path.unlink()
            shutil.rmtree(temp_dir, ignore_errors=True)

    except Exception as e:
        logger.exception("Unexpected error during file processing")
        flash(f"❌ Processing error: {str(e)}", "error")
        

    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# Batch Processing from Directory
# -----------------------------------------------------------------------------

@app.route("/process_directory", methods=["POST"])
@login_required
def process_directory():
    """Process all PDF files for a customer from configured directory."""
    customer_format = request.form.get("customer")
    
    # Validate customer
    available_customers = get_available_customers()
    if customer_format not in available_customers:
        flash(f"❌ Invalid customer format: {customer_format}", "error")
        return redirect(url_for("index"))

    try:
        po_directory = Path(Config.paths.PO_DIRECTORY)

        # Get customer's folder name from rules
        customer_folder = rules_config[customer_format].get("customer_folder", customer_format)
        customer_dir = po_directory / customer_folder

        if not customer_dir.exists():
            flash(f"❌ Directory not found: {customer_dir}", "error")
            logger.error(f"Customer directory not found: {customer_dir}")
            return redirect(url_for("index"))

        # Find all PDF files
        pdf_files = list(customer_dir.glob("*.pdf"))

        if not pdf_files:
            flash(f"⚠️ No PDF files found in {customer_dir}", "warning")
            return redirect(url_for("index"))

        logger.info(f"📂 Batch processing {len(pdf_files)} files for {customer_format}")

        results = []
        with time_operation(f"Batch Processing {customer_format}", logger=logger) as timer:
            # Process all files
            for pdf_file in pdf_files:
                result = po_service.process_file(pdf_file, customer_format)
                results.append(result)

                # Optionally save each to database
                if result.success and request.form.get("save_to_db") == "on":
                    db_integration.save_result(result)

        # Calculate summary
        success_count = sum(1 for r in results if r.success)
        total_lines = sum(len(r.lines) for r in results if r.success)

        # Store batch results in session
        session["batch_results"] = {
            "customer": customer_format,
            "total_files": len(results),
            "successful": success_count,
            "failed": len(results) - success_count,
            "total_lines": total_lines,
            "processing_time": timer.elapsed_time,
            "files": [
                {
                    "name": r.file_name,
                    "success": r.success,
                    "lines": len(r.lines) if r.success else 0,
                    "error": r.error_message if not r.success else None
                }
                for r in results
            ]
        }

        if success_count > 0:
            flash(f"✅ Batch complete. {success_count}/{len(results)} files processed ({total_lines} lines) in {timer.elapsed_time:.2f}s", "success")
            logger.info(f"Batch processing complete: {success_count}/{len(results)} successful")
        else:
            flash(f"❌ No files processed successfully", "error")
            logger.error("Batch processing failed: no successful files")

    except Exception as e:
        logger.exception("Batch processing failed")
        flash(f"❌ Batch processing error: {str(e)}", "error")

    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# API Endpoints (for JSON responses)
# -----------------------------------------------------------------------------

@app.route("/api/customers", methods=["GET"])
@login_required
def api_get_customers():
    """
    API endpoint to get available customers.
    Returns list of customers loaded from extraction rules.
    """
    try:
        customers = get_available_customers()
        return jsonify({
            'success': True,
            'customers': customers,
            'count': len(customers)
        })
    except Exception as e:
        logger.error(f"❌ API error getting customers: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route("/api/process", methods=["POST"])
@login_required
def api_process():
    """API endpoint to process file and return JSON."""
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"}), 400

    file = request.files["file"]
    customer_format = request.form.get("customer")

    if not file.filename or not allowed_file(file.filename):
        return jsonify({
            "success": False, 
            "error": f"Invalid file. Only {', '.join(Config.app.ALLOWED_EXTENSIONS)} allowed."
        }), 400

    # Validate customer
    available_customers = get_available_customers()
    if customer_format not in available_customers:
        return jsonify({
            "success": False, 
            "error": f"Invalid customer format. Available: {', '.join(available_customers)}"
        }), 400

    temp_dir = tempfile.mkdtemp()
    secure_name = secure_filename(file.filename)
    file_path = Path(temp_dir) / secure_name

    try:
        file.save(file_path)

        with time_operation(f"API Processing {secure_name}", logger=logger):
            result = po_service.process_file(file_path, customer_format)

        # Optionally save to database
        result_dict = result.to_dict()
        if result.success and request.form.get("save_to_db") == "true":
            db_result = db_integration.save_result(result)
            result_dict["database_saved"] = db_result["saved"]
            if not db_result["saved"]:
                result_dict["database_error"] = db_result.get("error")

        return jsonify(result_dict)

    except Exception as e:
        logger.exception("API processing failed")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@app.route("/api/formats", methods=["GET"])
@login_required
def api_formats():
    """Get list of supported customer formats (alias for /api/customers)."""
    try:
        customers = get_available_customers()
        return jsonify({
            "success": True,
            "formats": customers,
            "count": len(customers)
        })
    except Exception as e:
        logger.error(f"API error getting formats: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/api/result/<result_id>", methods=["GET"])
@login_required
def api_get_result(result_id):
    """Get processing result as JSON."""
    if result_id == "last":
        results = session.get("results")
        if results:
            return jsonify({
                "success": True,
                "result": results
            })
        return jsonify({
            "success": False,
            "error": "No result found in session"
        }), 404

    # In production, this would query the DB by result_id
    return jsonify({
        "success": False,
        "error": "Result ID lookup not implemented. Use 'last' for session result."
    }), 404

# -----------------------------------------------------------------------------
# Elior - Delivery Slip Generation
# -----------------------------------------------------------------------------

@app.route("/elior")
@login_required
def elior_page():
    """Display Elior delivery slip generation page."""
    return render_template(
        "elior.html",
        user=session.get("user"),
        auth_enabled=Config.app.AUTH_ENABLED,
        pdf_generated=session.get("pdf_generated", False),
    )

# -----------------------------------------------------------------------------
# Database Info
# -----------------------------------------------------------------------------

@app.route("/database_info")
@login_required
def database_info():
    """Display database statistics."""
    try:
        with db_service.get_session() as db_session:
            from models.models import Product, Customer, Mercuriale

            stats = {
                "products": db_session.query(Product).count(),
                "customers": db_session.query(Customer).count(),
                "mercuriales": db_session.query(Mercuriale).count(),
                "database_url": Config.database.DATABASE_URL.split('@')[-1] if '@' in Config.database.DATABASE_URL else Config.database.DATABASE_URL  # Hide credentials
            }

            return render_template(
                "database_info.html",
                user=session.get("user"),
                auth_enabled=Config.app.AUTH_ENABLED,
                stats=stats
            )
    except Exception as e:
        logger.exception("Failed to fetch database info")
        flash(f"❌ Database error: {e}", "error")
        return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# View Processing Result Details
# -----------------------------------------------------------------------------

@app.route("/result_details")
@login_required
def result_details():
    """Display detailed processing result."""
    results = session.get("results")
    if not results:
        flash("⚠️ No processing result available", "warning")
        return redirect(url_for("index"))

    return render_template(
        "result_details.html",
        user=session.get("user"),
        auth_enabled=Config.app.AUTH_ENABLED,
        results=results
    )


@app.route("/download_result_json")
@login_required
def download_result_json():
    """Download last processing result as JSON file."""
    results = session.get("results")
    if not results:
        flash("⚠️ No result to download", "warning")
        return redirect(url_for("index"))

    # Create temporary JSON file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8')
    json.dump(results, temp_file, indent=2, ensure_ascii=False)
    temp_file.close()

    filename = results.get('filename', 'unknown')
    download_name = f"po_result_{filename.rsplit('.', 1)[0]}.json"

    return send_file(
        temp_file.name,
        as_attachment=True,
        download_name=download_name,
        mimetype="application/json"
    )

# -----------------------------------------------------------------------------
# Error Handlers
# -----------------------------------------------------------------------------

@app.errorhandler(413)
def too_large(e):
    """Handle file too large error."""
    max_mb = Config.app.MAX_CONTENT_LENGTH / (1024 * 1024)
    flash(f"❌ File too large (max {max_mb:.0f}MB)", "error")
    logger.warning(f"File upload rejected: exceeds {max_mb}MB limit")
    return redirect(url_for("index"))


@app.errorhandler(404)
def not_found(e):
    """Handle 404 errors."""
    logger.warning(f"404 error: {request.url}")
    flash("⚠️ Page not found", "warning")
    return redirect(url_for("index"))


@app.errorhandler(500)
def internal_error(e):
    """Handle internal server error."""
    logger.error(f"Internal server error: {e}", exc_info=True)
    flash("❌ Internal server error", "error")
    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info(f"🚀 Starting Flask application on port 5000")
    logger.info(f"📋 Loaded {len(get_available_customers())} customer formats")
    
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=Config.app.DEBUG
    )