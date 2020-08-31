from flask import Flask, request, abort
from magic import Magic
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://test:test@localhost/fileLoader'

from extensions import db
db.init_app(app)

from models.UploadFile import UploadFile
from models.FileContent import FileContent
import pandas as pd

@app.route('/')
def hello():
    return "Hello Take Home Project!"

@app.route('/processUpload', methods=['POST'])
def process_upload():
    if not request.files:
        abort(400, "No file found")
    raw_file = request.files["file"]

    if raw_file.mimetype is not None and raw_file.mimetype != "text/csv":
        abort(400, f"Not an accepted mimetype: {raw_file.mimetype}")

    if raw_file.filename is None or raw_file.filename == "":
        abort(400, "No file selected")
    
    processed_file_details = _process_file(raw_file)
    _save_file_contents(raw_file, processed_file_details)

    return "Success", 200


# TODO: add hash to file name to return to the service caller
def get_storage_path(relative_storage_path):
    return os.path.join(os.path.dirname(__file__), relative_storage_path)


def get_file_size_in_bytes(file_path):
    return os.path.getsize(file_path)


def get_file_mime_type(file_name):
    mime = Magic(mime=True)
    return mime.from_file(file_name)


def _process_file(file) -> UploadFile:
    file_name = file.filename
    some_path = os.path.abspath(file_name)
    file_dir = os.path.dirname(some_path)
    file_path = os.path.join(file_dir, 'data', file_name)

    try:
        upload_file = UploadFile(
            name=file_name,
            storage_path=get_storage_path('data/processed'),
            file_size_bytes=get_file_size_in_bytes(file_path),
            mime_type="text/csv",  # TODO: handle other types later
        )
        upload_file.save()
        return upload_file
    except Exception as e:
        print(e)
        raise

    return upload_file

def _save_file_contents(raw_file, file_details: UploadFile):
    data_frame = pd.read_csv(raw_file)
    file_content = FileContent(
        upload_file_id=file_details.id,
        name=file_details.name,
        content=data_frame,
    )
    try:
        file_content.save()
    except Exception as e:
        print(e)
        raise

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
    