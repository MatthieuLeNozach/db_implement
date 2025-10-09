# app.py - Updated Flask Integration
import os
import tempfile
import shutil
import json
from functools import wraps
from pathlib import Path
import logging # Still needed for type hinting/misc logging

from flask import (
    Flask, render_template, request, redirect,
    send_file, url_for, flash, session, jsonify
)
from flask_session import Session as FlaskSession
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

# NEW: Import Core Components
from src.core.config import Config
from src.core.logging import setup_logging, time_operation
from src.core.exceptions import FileNotFoundError, AuthenticationError, DatabaseConnectionError, ConfigurationError

# NEW: Import refactored service
from src.services.purchase_order_service import (
    PurchaseOrderService,
    ExtractionRulesLoader,
    DatabaseIntegration
)
# Assuming this service is correctly located/imported
from services.database_service import DatabaseService 

# -----------------------------------------------------------------------------
# App setup - UTILIZING CENTRALIZED CONFIG
# -----------------------------------------------------------------------------

# 1. Initialize core configuration (Loads .env, creates dirs, validates)
# NOTE: The load_dotenv() call is now inside src/core/config.py
try:
    Config.initialize()
except ConfigurationError as e:
    # Handle critical configuration failure early
    print(f"FATAL CONFIGURATION ERROR: {e}")
    exit(1)


app = Flask(__name__, static_folder='static', static_url_path='/static')

# 2. Apply centralized Flask configuration
app.config.update(Config.get_flask_config())

# 3. Setup logging with central module, using Config settings
# Sets up 'logger' instance and configures root logging based on VERBOSE
logger = setup_logging(verbose=Config.app.VERBOSE)
logger.info(Config.summary())

# Ensure required directories exist (already done by Config.initialize(), but good for safety)
Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)
Path(app.config["PO_DIRECTORY"]).mkdir(parents=True, exist_ok=True)

FlaskSession(app)

# Initialize services
db_service = DatabaseService()
logger.info("🔧 Initializing Purchase Order Service...")

# Load extraction rules
rules_path = Path(app.config["RULES_CSV_PATH"])
if not rules_path.exists():
    logger.error(f"❌ Rules CSV not found: {rules_path}")
    raise FileNotFoundError(f"Extraction rules not found: {rules_path}")

try:
    # Using the path from Config/Flask app config
    rules_config = ExtractionRulesLoader.load_from_csv(rules_path) 
except Exception as e:
    logger.error(f"❌ Failed to load extraction rules: {e}")
    raise

po_service = PurchaseOrderService()
db_integration = DatabaseIntegration(db_service)

logger.info(f"✅ Service initialized with {len(rules_config)} customer formats")

# Verify database connection at startup
try:
    with db_service.get_session() as db_session:
        # Assuming models are available
        from models.models import Product 
        product_count = db_session.query(Product).count()
        logger.info(f"✅ Database connected. Products: {product_count}")
except Exception as e:
    logger.critical(f"❌ Database connection failed: {e}")
    raise DatabaseConnectionError(f"Startup database check failed: {e}")

# -----------------------------------------------------------------------------
# Auth setup
# -----------------------------------------------------------------------------
# Use values from Config
BASIC_AUTH_USERS = {
    "admin": {"password_hash": generate_password_hash(Config.app.DEFAULT_USER_PWD)}
}
# Use value from Config
ALLOWED_EXTENSIONS = Config.app.ALLOWED_EXTENSIONS

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed (PDF only)."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def login_required(f):
    """Decorator to require login for routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not Config.app.AUTH_ENABLED or session.get("user"):
            return f(*args, **kwargs)
        # Auth is enabled but user is not in session
        return redirect(url_for("login"))
    return decorated_function

# -----------------------------------------------------------------------------
# Auth routes
# -----------------------------------------------------------------------------
@app.route("/ping")
def ping():
    """Health check endpoint."""
    return jsonify({"status": "pong", "env": Config.app.ENV})


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
        flash("Please enter both username and password")
        return redirect(url_for("login"))

    user = BASIC_AUTH_USERS.get(username)
    if user and check_password_hash(user["password_hash"], password):
        session["user"] = {"name": username, "auth_type": "basic"}
        flash(f"Logged in as {username}")
        return redirect(url_for("index"))

    flash("Invalid credentials")
    raise AuthenticationError("Login attempt failed with invalid credentials.")


@app.route("/logout")
def logout():
    """Handle logout."""
    session.clear()
    flash("Logged out successfully")
    return redirect(url_for("login"))

# -----------------------------------------------------------------------------
# Main dashboard
# -----------------------------------------------------------------------------
@app.route("/")
@login_required
def index():
    """Display main dashboard."""
    last_result = session.get("last_result")
    
    return render_template(
        "index.html",
        user=session.get("user"),
        auth_enabled=Config.app.AUTH_ENABLED, # Use Config
        result=last_result,
        supported_customers=list(rules_config.keys()),
        batch_results=session.get("batch_results")
    )


@app.route("/clear_results", methods=["POST"])
@login_required
def clear_results():
    """Clear session results."""
    session.pop("last_result", None)
    session.pop("batch_results", None)
    flash("Results cleared", "info")
    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# Single File Upload & Processing
# -----------------------------------------------------------------------------
@app.route("/upload_file", methods=["POST"])
@login_required
def upload_file():
    """Handle single file upload and process purchase order PDF."""
    if "file" not in request.files:
        flash("No file uploaded", "error")
        return redirect(url_for("index"))

    file = request.files["file"]
    if file.filename == "":
        flash("No file selected", "error")
        return redirect(url_for("index"))

    if not allowed_file(file.filename):
        flash(f"Only {', '.join(Config.app.ALLOWED_EXTENSIONS)} files are allowed", "error")
        return redirect(url_for("index"))

    customer_format = request.form.get("customer")
    if customer_format not in rules_config:
        flash("Invalid or missing customer format", "error")
        return redirect(url_for("index"))

    # Save to temporary file
    temp_dir = tempfile.mkdtemp()
    secure_name = secure_filename(file.filename)
    file_path = Path(temp_dir) / secure_name
    
    try:
        file.save(file_path)

        logger.info(f"📁 Processing uploaded file: {secure_name} for format: {customer_format}")

        # Use time_operation from logging module
        with time_operation(f"Processing {secure_name}", logger=logger) as timer:
            # Process with new service
            result = po_service.process_file(file_path, customer_format)

        if result.success:
            # Store in session for display
            session["last_result"] = result.to_dict()
            
            # Optionally save to database
            save_to_db = request.form.get("save_to_db") == "on"
            if save_to_db:
                db_result = db_integration.save_result(result)
                if db_result["saved"]:
                    flash(f"✅ Successfully processed and saved: {secure_name}", "success")
                else:
                    flash(f"⚠️ Processed but database save failed: {db_result['error']}", "warning")
            else:
                flash(f"✅ Successfully processed: {secure_name} in {timer.elapsed_time:.2f}s", "success")
            
            logger.info(f"✅ File processed: {len(result.lines)} lines extracted")
        else:
            flash(f"❌ Processing failed: {result.error_message}", "error")
            logger.error(f"Processing failed for {secure_name}: {result.error_message}")

    except Exception as e:
        logger.exception("Unexpected error during file processing")
        flash(f"❌ Processing error: {str(e)}", "error")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# Batch Processing from Directory
# -----------------------------------------------------------------------------
@app.route("/process_directory", methods=["POST"])
@login_required
def process_directory():
    """Process all PDF files for a customer from configured directory."""
    customer_format = request.form.get("customer")
    if customer_format not in rules_config:
        flash("Invalid customer format", "error")
        return redirect(url_for("index"))
    
    try:
        # Use value from Config
        po_directory = Path(Config.paths.PO_DIRECTORY) 
        
        # Get customer's folder name from rules
        customer_folder = rules_config[customer_format].get("customer_folder", customer_format)
        customer_dir = po_directory / customer_folder
        
        if not customer_dir.exists():
            flash(f"❌ Directory not found: {customer_dir}", "error")
            return redirect(url_for("index"))
        
        # Find all PDF files
        pdf_files = list(customer_dir.glob("*.pdf"))
        
        if not pdf_files:
            flash(f"No PDF files found in {customer_dir}", "warning")
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
        else:
            flash(f"❌ No files processed successfully", "error")
    
    except Exception as e:
        logger.exception("Batch processing failed")
        flash(f"❌ Batch processing error: {str(e)}", "error")
    
    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# API Endpoints (for JSON responses)
# -----------------------------------------------------------------------------
@app.route("/api/process", methods=["POST"])
@login_required
def api_process():
    """API endpoint to process file and return JSON."""
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"}), 400

    file = request.files["file"]
    customer_format = request.form.get("customer")
    
    if not file.filename or not allowed_file(file.filename):
        return jsonify({"success": False, "error": f"Invalid file. Only {', '.join(Config.app.ALLOWED_EXTENSIONS)} allowed."}), 400
    
    if customer_format not in rules_config:
        return jsonify({"success": False, "error": "Invalid customer format"}), 400

    temp_dir = tempfile.mkdtemp()
    secure_name = secure_filename(file.filename)
    file_path = Path(temp_dir) / secure_name
    
    try:
        file.save(file_path)

        with time_operation(f"API Processing {secure_name}"):
            result = po_service.process_file(file_path, customer_format)
        
        # Optionally save to database
        if result.success and request.form.get("save_to_db") == "true":
            db_result = db_integration.save_result(result)
            result_dict = result.to_dict()
            result_dict["database_saved"] = db_result["saved"]
            return jsonify(result_dict)
        
        return jsonify(result.to_dict())

    except Exception as e:
        logger.exception("API processing failed")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@app.route("/api/formats", methods=["GET"])
@login_required
def api_formats():
    """Get list of supported customer formats."""
    return jsonify({
        "formats": list(rules_config.keys()),
        "count": len(rules_config)
    })


@app.route("/api/result/<result_id>", methods=["GET"])
@login_required
def api_get_result(result_id):
    """Get processing result as JSON."""
    # NOTE: Since this is fetching from session (not DB), it will only get 'last_result'
    last_result = session.get("last_result")
    if last_result and result_id == "last": # Added 'last' check for session access
        return jsonify(last_result)
    
    # In a production app, this would query the DB by result_id
    return jsonify({"error": "No result found or ID not supported"}), 404


# -----------------------------------------------------------------------------
# Elior - Delivery Slip Generation
# -----------------------------------------------------------------------------
@app.route("/elior")
@login_required
def elior_page():
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
            # Assuming models are available
            from models.models import Product, Customer, Mercuriale 
            
            stats = {
                "products": db_session.query(Product).count(),
                "customers": db_session.query(Customer).count(),
                "mercuriales": db_session.query(Mercuriale).count(),
                "database_url": Config.database.DATABASE_URL # Use Config
            }
            
            return render_template(
                "database_info.html",
                user=session.get("user"),
                auth_enabled=Config.app.AUTH_ENABLED,
                stats=stats
            )
    except Exception as e:
        logger.exception("Failed to fetch database info")
        flash(f"Database error: {e}", "error")
        return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# View Processing Result Details
# -----------------------------------------------------------------------------
@app.route("/result_details")
@login_required
def result_details():
    """Display detailed processing result."""
    result = session.get("last_result")
    if not result:
        flash("No processing result available", "warning")
        return redirect(url_for("index"))
    
    return render_template(
        "result_details.html",
        user=session.get("user"),
        auth_enabled=Config.app.AUTH_ENABLED,
        result=result
    )


@app.route("/download_result_json")
@login_required
def download_result_json():
    """Download last processing result as JSON file."""
    result = session.get("last_result")
    if not result:
        flash("No result to download", "warning")
        return redirect(url_for("index"))
    
    # Create temporary JSON file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    # Using json.dump with ensure_ascii=False for proper characters
    json.dump(result, temp_file, indent=2, ensure_ascii=False)
    temp_file.close()
    
    return send_file(
        temp_file.name,
        as_attachment=True,
        download_name=f"po_result_{result.get('file_name', 'unknown')}.json",
        mimetype="application/json"
    )

# -----------------------------------------------------------------------------
# Error handlers
# -----------------------------------------------------------------------------
@app.errorhandler(413)
def too_large(e):
    """Handle file too large error."""
    # Use Config for max size reference
    max_mb = Config.app.MAX_CONTENT_LENGTH / (1024 * 1024)
    flash(f"File too large (max {max_mb:.0f}MB)", "error")
    return redirect(url_for("index"))


@app.errorhandler(500)
def internal_error(e):
    """Handle internal server error."""
    logger.error(f"Internal server error: {e}")
    flash("Internal server error", "error")
    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Directories are handled by Config.initialize(), but run is here.
    # Note: Flask's built-in server is used for debug/development.
    app.run(
        host="0.0.0.0", 
        port=5000, 
        debug=Config.app.DEBUG # Use Config for debug flag
    )