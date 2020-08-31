from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy import Boolean, ForeignKey, INTEGER, String, TIMESTAMP
from sqlalchemy.exc import DataError
from uuid import uuid4
import os


from extensions import db
from db.mixins.basic_crud_mixin import BasicCrudMixin


class UploadFile(db.Model, BasicCrudMixin):
    __tablename__ = "upload_file"

    id = db.Column("id", UUID(as_uuid=True), primary_key=True)
    created_utc = db.Column(
        "created_utc", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )

    modified_utc = db.Column(
        "modified_utc",
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
        onupdate=func.now(),
    )

    file_size_bytes = db.Column("file_size_bytes", INTEGER, nullable=False)
    mime_type = db.Column("mime_type", String, nullable=True)
    name = db.Column("name", String, nullable=False)
    storage_path = db.Column("storage_path", String, nullable=False)




    #data_frames = db.relationship(
    #    "DataFrameMetadata", backref=db.backref("upload_file", lazy=True), cascade="all, delete-orphan"
    #)

    #events = db.relationship("UploadEvent")

    #errors = db.relationship(
    #    "UploadError", backref=db.backref("upload_file", lazy=True), cascade="all, delete-orphan"
    #)

    def __init__(self, **kwargs):
        if kwargs is None:
            return
        self.id = kwargs.get("id", uuid4())
        self.created_utc = kwargs.get("created_utc", datetime.utcnow())
        self.modified_utc = kwargs.get("modified_utc", datetime.utcnow())
        self.file_size_bytes = kwargs.get("file_size_bytes")
        self.mime_type = kwargs.get("mime_type")
        self.name = kwargs.get("name")
        self.storage_path = kwargs.get("storage_path")
        self.move_file_to_storage()

    def move_file_to_storage(self):
        file_dir = os.path.dirname(self.name)
        file_path_relative = os.path.join(file_dir, 'data', self.name)
        file_path_abs = os.path.abspath(file_path_relative)
        file_new_path = os.path.join(self.storage_path, self.name)

        # move file to storage path
        os.replace(file_path_abs, file_new_path)

    def recursive_delete(self):
        try:
            children_upload_files = (db.session.query(UploadFile)
                                     .filter(UploadFile.id == self.id)
                                     .cte(name='children_upload_files',
                                     recursive=True))
            children_upload_files = children_upload_files.union_all(
                db.session.query(UploadFile).
                filter(
                    UploadFile.parent_upload_file_id ==
                    children_upload_files.c.id)
            )
            for child in db.session.query(UploadFile).\
                    filter(UploadFile.id == children_upload_files.c.id):
                db.session.delete(child)
            db.session.commit()
        except DataError:
            # on rollback, the same closure of state
            # as that of commit proceeds.
            db.session.rollback()
            raise
