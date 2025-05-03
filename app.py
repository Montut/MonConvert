import os
import subprocess
import uuid
import time
import logging
from collections import defaultdict
from functools import wraps
from flask import Flask, request, send_from_directory, jsonify, after_this_request
from werkzeug.utils import secure_filename

# --- Configuration ---
# Ensure Flask is installed: pip install Flask
# Ensure ffmpeg is installed and accessible in the system PATH.

UPLOAD_FOLDER = 'uploads'       # Temporary storage for uploaded files
CONVERTED_FOLDER = 'converted'  # Temporary storage for converted files
ALLOWED_EXTENSIONS = {'mp4', 'avi', 'mov', 'wmv', 'flv', 'mkv', 'webm', 'mpeg', 'mpg', 'm4v'} # Common video formats
MAX_FILE_SIZE_MB = 100          # Max upload size in Megabytes
RATE_LIMIT_REQUESTS = 5         # Max requests per window
RATE_LIMIT_WINDOW_SECONDS = 60  # Time window in seconds (1 minute)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['CONVERTED_FOLDER'] = CONVERTED_FOLDER
# Convert MB to Bytes for Flask config
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE_MB * 1024 * 1024

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- In-memory Rate Limiting Store ---
# Stores IP address -> list of request timestamps
request_timestamps = defaultdict(list)

# --- Helper Functions ---

def allowed_file(filename):
    """Checks if the file extension is allowed."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_temp_dirs():
    """Creates temporary directories if they don't exist."""
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(CONVERTED_FOLDER, exist_ok=True)

def cleanup_file(filepath):
    """Safely removes a file if it exists."""
    if filepath and os.path.exists(filepath):
        try:
            os.remove(filepath)
            logging.info(f"Cleaned up temporary file: {filepath}")
        except OSError as e:
            logging.error(f"Error removing temporary file '{filepath}': {e}")

# --- Decorator for Rate Limiting ---
def rate_limit(limit, window):
    """Decorator factory for rate limiting."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            ip_address = request.remote_addr
            now = time.time()

            # Remove timestamps older than the window
            request_timestamps[ip_address] = [ts for ts in request_timestamps[ip_address] if now - ts < window]

            # Check if limit is exceeded
            if len(request_timestamps[ip_address]) >= limit:
                logging.warning(f"Rate limit exceeded for IP: {ip_address}")
                return jsonify({"error": f"Rate limit exceeded. Please wait {window} seconds."}), 429 # Too Many Requests

            # Record the current request timestamp
            request_timestamps[ip_address].append(now)
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# --- Routes ---

@app.route('/')
def index():
    """Serves the main HTML page."""
    # Serve index.html from the current directory
    try:
        return send_from_directory('.', 'index.html')
    except FileNotFoundError:
        logging.error("index.html not found in the current directory.")
        return "Error: index.html not found.", 404
    except Exception as e:
        logging.error(f"Error serving index.html: {e}")
        return "An internal error occurred.", 500


@app.route('/upload', methods=['POST'])
@rate_limit(limit=RATE_LIMIT_REQUESTS, window=RATE_LIMIT_WINDOW_SECONDS) # Apply rate limiting
def upload_file():
    """Handles file upload, validation, conversion, and cleanup."""
    create_temp_dirs() # Ensure directories exist

    upload_path = None      # Initialize path variables for cleanup
    converted_path = None

    try:
        # --- 1. File Existence and Basic Validation ---
        if 'videoFile' not in request.files:
            logging.warning("No file part in request.")
            return jsonify({"error": "No file part provided."}), 400
        file = request.files['videoFile']

        if file.filename == '':
            logging.warning("No selected file.")
            return jsonify({"error": "No file selected."}), 400

        # --- 2. File Type Validation ---
        if not allowed_file(file.filename):
            logging.warning(f"Invalid file type uploaded: '{file.filename}'")
            return jsonify({"error": f"Invalid file type. Allowed types: {', '.join(ALLOWED_EXTENSIONS)}"}), 400

        # --- 3. File Size Validation (Check against MAX_CONTENT_LENGTH) ---
        # Flask usually handles this via MAX_CONTENT_LENGTH, but an explicit check adds clarity.
        # Note: request.content_length might be None if not provided by the client/server config.
        if request.content_length and request.content_length > app.config['MAX_CONTENT_LENGTH']:
             logging.warning(f"File size exceeded limit ({MAX_FILE_SIZE_MB} MB) for IP: {request.remote_addr}")
             return jsonify({"error": f"File is too large. Maximum size is {MAX_FILE_SIZE_MB} MB."}), 413 # Payload Too Large

        # --- 4. Sanitize and Prepare Filenames ---
        original_filename = secure_filename(file.filename)
        unique_id = str(uuid.uuid4())
        # Keep original extension for potential ffmpeg format detection, but output is always mp4
        original_extension = original_filename.rsplit('.', 1)[1].lower() if '.' in original_filename else ''
        temp_filename = f"{unique_id}.{original_extension}" # Use original extension for input temp file
        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)

        # Define the output filename (always MP4)
        converted_filename = f"{unique_id}.mp4" # Output is always mp4
        converted_path = os.path.join(app.config['CONVERTED_FOLDER'], converted_filename)

        # --- 5. Save Uploaded File ---
        file.save(upload_path)
        logging.info(f"File '{original_filename}' uploaded as '{temp_filename}' by IP: {request.remote_addr}.")

        # --- 6. Get Conversion Options ---
        compression = request.form.get('compression', 'medium').lower() # Default to medium
        resolution = request.form.get('resolution', 'original').lower() # Default to original

        # --- 7. Construct FFmpeg Command ---
        ffmpeg_command = ['ffmpeg', '-i', upload_path]

        # Add compression flags (-crf)
        # Lower CRF = better quality, larger file. Higher CRF = lower quality, smaller file.
        if compression == 'low':    # Best quality
            ffmpeg_command.extend(['-crf', '18'])
        elif compression == 'high': # Smaller file
            ffmpeg_command.extend(['-crf', '28'])
        else: # Medium (default)
            ffmpeg_command.extend(['-crf', '23'])

        # Add resolution flags (-vf scale)
        # -vf scale=-2:H scales height to H, maintains aspect ratio, width divisible by 2
        if resolution == '1080p':
            ffmpeg_command.extend(['-vf', 'scale=-2:1080'])
        elif resolution == '720p':
            ffmpeg_command.extend(['-vf', 'scale=-2:720'])
        elif resolution == '480p':
            ffmpeg_command.extend(['-vf', 'scale=-2:480'])
        # 'original' needs no scaling flag

        # Add standard output options
        ffmpeg_command.extend([
            '-codec:v', 'libx264', # Common video codec
            '-preset', 'medium',   # Encoding speed/compression balance
            '-codec:a', 'aac',     # Common audio codec
            '-b:a', '128k',        # Audio bitrate
            '-movflags', '+faststart', # Optimize for web streaming
            '-y',                  # Overwrite output without asking
            '-hide_banner',        # Hide FFmpeg startup banner
            '-loglevel', 'error',  # Show only errors
            converted_path         # Output file path
        ])

        # --- 8. Execute FFmpeg Conversion ---
        logging.info(f"Starting conversion for '{temp_filename}' (Options: {compression}, {resolution}). Command: {' '.join(ffmpeg_command)}")
        process = subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
        logging.info(f"FFmpeg conversion successful for '{temp_filename}'. Output: '{converted_filename}'")

        # --- 9. Return Success Response ---
        # Uploaded file will be cleaned up in the finally block
        return jsonify({"filename": converted_filename}), 200

    except subprocess.CalledProcessError as e:
        # Handle FFmpeg errors specifically
        logging.error(f"FFmpeg conversion failed for '{temp_filename}'. Error: {e}")
        logging.error(f"FFmpeg stderr: {e.stderr}")
        # Cleanup happens in finally block
        return jsonify({"error": "Video conversion failed. The format might be unsupported or the file corrupted."}), 500
    except Exception as e:
        # Handle other unexpected errors
        logging.error(f"An unexpected error occurred during upload/conversion: {e}", exc_info=True) # Log traceback
        # Cleanup happens in finally block
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        # --- 10. Robust Cleanup ---
        # Always attempt to remove the original uploaded file
        cleanup_file(upload_path)
        # If conversion failed and converted file exists (partially created), remove it
        if 'process' in locals() and process.returncode != 0:
             cleanup_file(converted_path)
        # If an exception occurred *before* conversion started, converted_path might still need cleanup check
        elif 'process' not in locals():
             cleanup_file(converted_path)


@app.route('/download/<filename>')
def download_file(filename):
    """Provides the converted file for download and cleans it up afterwards."""
    # --- 1. Sanitize Filename ---
    # IMPORTANT: Security - ensure filename doesn't allow directory traversal.
    safe_filename = secure_filename(filename)
    if safe_filename != filename:
        logging.warning(f"Attempted download with potentially unsafe filename: '{filename}' by IP: {request.remote_addr}")
        return "Invalid filename", 400

    file_path = os.path.join(app.config['CONVERTED_FOLDER'], safe_filename)

    # --- 2. Check File Existence ---
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        logging.error(f"Download request for non-existent or invalid file: '{safe_filename}' by IP: {request.remote_addr}")
        return "File not found.", 404

    try:
        logging.info(f"Sending file '{safe_filename}' for download to IP: {request.remote_addr}.")

        # --- 3. Schedule Cleanup After Request ---
        # Use Flask's after_this_request to delete the file *after* it's sent.
        @after_this_request
        def remove_file(response):
            cleanup_file(file_path)
            return response

        # --- 4. Send File ---
        return send_from_directory(app.config['CONVERTED_FOLDER'], safe_filename, as_attachment=True)

    except Exception as e:
        logging.error(f"Error sending file '{safe_filename}': {e}")
        return "An error occurred while downloading the file.", 500

# --- Main Execution ---
if __name__ == '__main__':
    create_temp_dirs() # Create dirs on startup
    # For production: Use a proper WSGI server like Gunicorn or Waitress behind a reverse proxy (Nginx/Apache)
    # Example: gunicorn -w 4 -b 0.0.0.0:5000 app:app
    # Development server:
    app.run(debug=False, host='0.0.0.0', port=5000) # Turn debug OFF for production-like testing
