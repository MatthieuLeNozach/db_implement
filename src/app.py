import os
import logging
import tempfile
import shutil
from functools import wraps
from pathlib import Path

from flask import (
    Flask, render_template, request, redirect,
    send_file, url_for, flash, session
)
from flask_session import Session
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv
from sqlalchemy import text

# New architecture imports
from core.formats import FORMATS
from core.logging import setup_logging
from services.purchase_order_pipeline_service import PurchaseOrderPipeline
#from processors.document_processor import DeliverySlipProcessor
#from processors.document_generator import DeliverySlipGenerator
from core.constants import BCColumns

from services.database_service import DatabaseService

db_service = DatabaseService()

with db_service.get_session() as db_session:  # Rename here
    result = db_session.execute(text("SELECT * FROM product")).fetchall()

# -----------------------------------------------------------------------------
# App setup
# -----------------------------------------------------------------------------
load_dotenv(".env")

app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.environ.get("SECRET_KEY", "change-me-in-production"),
    UPLOAD_FOLDER="uploads",
    MAX_CONTENT_LENGTH=16 * 1024 * 1024,  # 16MB
    SESSION_TYPE="filesystem",
    SESSION_PERMANENT=False,
    SESSION_USE_SIGNER=True,
    AUTH_ENABLED=os.environ.get("AUTH_ENABLED", "true").lower() == "true",
    VERBOSE=True,
)

logger = setup_logging(verbose=app.config["VERBOSE"])
Session(app)

# -----------------------------------------------------------------------------
# Auth setup
# -----------------------------------------------------------------------------
BASIC_AUTH_USERS = {
    "admin": {"password_hash": generate_password_hash(os.environ["DEFAULT_USER_PWD"])}
}
ALLOWED_EXTENSIONS = {"pdf", "csv"}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not app.config["AUTH_ENABLED"] or session.get("user"):
            return f(*args, **kwargs)
        return redirect(url_for("login"))
    return decorated_function

# -----------------------------------------------------------------------------
# Auth routes
# -----------------------------------------------------------------------------
@app.route("/ping")
def ping():
    return "pong"


@app.route("/login", methods=["GET"])
def login():
    if session.get("user"):
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/login", methods=["POST"])
def login_basic():
    username = request.form.get("username")
    password = request.form.get("password")

    if not username or not password:
        flash("Please enter both username and password")
        return redirect(url_for("login"))

    user = BASIC_AUTH_USERS.get(username)
    if user and check_password_hash(user["password_hash"], password):
        session["user"] = {"name": username, "auth_type": "basic"}
        flash(f"✅ Logged in as {username}")
        return redirect(url_for("index"))

    flash("❌ Invalid credentials")
    return redirect(url_for("login"))


@app.route("/logout")
def logout():
    session.clear()
    flash("✅ Logged out successfully")
    return redirect(url_for("login"))

# -----------------------------------------------------------------------------
# Main dashboard
# -----------------------------------------------------------------------------
@app.route("/")
@login_required
def index():
    results = session.get("last_results")
    return render_template(
        "index.html",
        user=session.get("user"),
        auth_enabled=app.config["AUTH_ENABLED"],
        results=results,
        supported_customers=list(FORMATS.keys()),
    )


@app.route("/clear_results", methods=["POST"])
@login_required
def clear_results():
    session.pop("last_results", None)
    flash("🧹 Results cleared", "info")
    return redirect(url_for("index"))

# -----------------------------------------------------------------------------
# File Upload & Processing
# -----------------------------------------------------------------------------
@app.route("/upload_file", methods=["POST"])
@login_required
def upload_file():
    if "file" not in request.files:
        flash("❌ No file uploaded")
        return redirect(url_for("index"))

    file = request.files["file"]
    if file.filename == "":
        flash("❌ No file selected")
        return redirect(url_for("index"))

    if not allowed_file(file.filename):
        flash("❌ Only PDF or CSV files are allowed")
        return redirect(url_for("index"))

    customer = request.form.get("customer")
    if customer not in FORMATS:
        flash("❌ Invalid or missing customer")
        return redirect(url_for("index"))

    temp_dir = tempfile.mkdtemp()
    try:
        customer_dir = Path(temp_dir) / customer
        customer_dir.mkdir(exist_ok=True)

        secure_name = secure_filename(file.filename)
        file_path = customer_dir / secure_name
        file.save(file_path)

        logger.info(f"📁 Saved uploaded file to {file_path}")

        # ✅ Use new pipeline API
        pipeline = PurchaseOrderPipeline(
            formats_config={customer: FORMATS[customer]},
            base_po_directory=Path(temp_dir),
            base_db_directory=app.config["DB_DIR"],
        )

        result = pipeline.process_single_file(file_path, customer)

        if result.success:
            session["last_results"] = format_result_for_ui(result, customer, secure_name)
            flash(f"✅ Successfully processed {secure_name} for {customer}", "success")
        else:
            flash(f"❌ Processing failed: {result.error_message}", "error")

    except Exception as e:
        logger.exception("❌ Pipeline execution failed")
        flash(f"❌ Pipeline error: {e}", "error")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.info(f"🧹 Cleaned up {temp_dir}")

    return redirect(url_for("index"))


def format_result_for_ui(result, customer: str, filename: str) -> dict:
    return {
        "customer": customer,
        "timestamp": filename,
        "summary": {
            "header_info": getattr(result, "header_info", {}) or {},
            "processing_summary": result.stats or {},
            "clean_data": result.clean_df.to_dict("records") if result.clean_df is not None else [],
            "faulty_data": result.faulty_df.to_dict("records") if result.faulty_df is not None else [],
        },
    }

# -----------------------------------------------------------------------------
# Elior - Delivery Slip Generation
# -----------------------------------------------------------------------------
@app.route("/elior")
@login_required
def elior_page():
    return render_template(
        "elior.html",
        user=session.get("user"),
        auth_enabled=app.config["AUTH_ENABLED"],
        pdf_generated=session.get("pdf_generated", False),
    )


# @app.route("/generate_delivery_slip", methods=["POST"])
# @login_required
# def generate_delivery_slip():
#     if "excel_file" not in request.files:
#         return "❌ Aucun fichier Excel uploadé", 400

#     file = request.files["excel_file"]
#     if file.filename == "":
#         return "❌ Aucun fichier sélectionné", 400

#     cdpf_number = request.form.get("cdpf_number")
#     site_name = request.form.get("site_name")
#     delivery_date = request.form.get("delivery_date")
#     order_number = request.form.get("order_number")

#     processor = DeliverySlipProcessor(generator_cls=DeliverySlipGenerator)
#     pdf_path = processor.process(
#         file, cdpf_number, site_name, delivery_date, order_number,
#         mercuriale_dir=app.config["MERCURIALE_DIR"]
#     )

#     if not pdf_path or not pdf_path.exists():
#         return "❌ Le PDF n'a pas été généré", 500

#     return send_file(pdf_path, as_attachment=True, download_name="bon_elior.pdf", mimetype="application/pdf")


# -----------------------------------------------------------------------------
# Error handlers
# -----------------------------------------------------------------------------
@app.errorhandler(413)
def too_large(e):
    flash("❌ File too large (max 16MB)", "error")
    return redirect(url_for("index"))


@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {e}")
    flash("❌ Internal server error", "error")
    return redirect(url_for("index"))


# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    app.run(host="0.0.0.0", port=5000, debug=True)
