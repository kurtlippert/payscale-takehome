import uuid
import time
from datetime import datetime

import pandas as pd
from pandas.io.sql import SQLDatabase, SQLTable
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    INTEGER,
    JSON,
    String,
    TIMESTAMP,
)

from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.exc import DataError, ProgrammingError
from sqlalchemy.sql import func, text
from sqlalchemy.sql.expression import TextClause

from extensions import db
from db.mixins.basic_crud_mixin import BasicCrudMixin

# max number of rows to query to/from db at a time, recommended ~10000
CHUNK_SIZE = 8192  # 1024 * 8
DATA_FRAME_CONTENT_SCHEMA = "data_frame_content"
DATA_FRAME_CONTENT_INDEX_HEADER = "__index__"
METADATA_HEADER = "__metadata__"
RECORD_IDS_HEADER = "__record_ids__"


class DataFrameSaveError(Exception):
    """Base exception for errors arising from saving data frame metadata and content."""
    pass


class DataFrameMetadataSaveError(DataFrameSaveError):
    """Error while attempting to write metadata to the 'file_content' table."""
    pass

class DataFrameMutableContentSaveError(DataFrameSaveError):
    """Error while attempting to persist mutable uploaded content."""
    pass


class FileContent(db.Model, BasicCrudMixin):
    __tablename__ = "file_content"

    id = db.Column("id", UUID(as_uuid=True), primary_key=True)
    upload_file_id = db.Column(
        "upload_file_id", UUID(as_uuid=True), ForeignKey("upload_file.id"), nullable=False
    )
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
    category = db.Column("category", String, nullable=True)

    name = db.Column("name", String, nullable=False)
    number_of_columns = db.Column("number_of_columns", INTEGER, nullable=False)
    record_count = db.Column("record_count", INTEGER, nullable=False)
    content_headers = db.Column("content_headers", ARRAY(String), nullable=False)


    def __init__(
        self,
        *,
        upload_file_id: uuid.UUID,
        name: str,
        content: pd.DataFrame,
        **kwargs,
    ):
        self.upload_file_id = upload_file_id
        self.name = name

        self.id = kwargs.get("id", uuid.uuid4())

        # Cast all columns in content to string type.
        if not content.dtypes.eq(object).all():
            content = content.fillna("").astype(str)

        # Internal cache of content. On init this won't be None, but it can be
        # invalidated, and in general will be lost when the data_frame instance
        # goes out of scope.
        self._content = self._add_record_ids(content)

        print(self._content)
        self.content_headers = list(self._content.columns)
        self.record_count = kwargs.get("record_count") or content.shape[0]
        self.number_of_columns = kwargs.get("number_of_columns") or content.shape[1]

        self.created_utc = kwargs.get("created_utc", datetime.utcnow())
        self.modified_utc = kwargs.get("modified_utc", datetime.utcnow())

        self.category = kwargs.get("category", "employee")

        self._should_update_db_content = True
        self._saved_original_content = False


    @property
    def _content_table(self):
        if self._content is not None:
            sql_table = SQLTable(
                name=self.schema_table_name,
                pandas_sql_engine=self._sql_engine,
                frame=self._content,
                if_exists="replace",
                index=False,
                schema=DATA_FRAME_CONTENT_SCHEMA,
            )
            index_col = Column(DATA_FRAME_CONTENT_INDEX_HEADER, INTEGER, primary_key=True, autoincrement=True)
            sql_table.table.append_column(index_col)
            metadata_col = Column(METADATA_HEADER, JSONB, nullable=False, server_default="{}")
            sql_table.table.append_column(metadata_col)
        else:
            sql_table = SQLTable(
                name=self.schema_table_name,
                pandas_sql_engine=self._sql_engine,
                if_exists="replace",
                index=True,
                index_label=DATA_FRAME_CONTENT_INDEX_HEADER,
                schema=DATA_FRAME_CONTENT_SCHEMA,
            )
        return sql_table


    @property
    def _sql_engine(self):
        return SQLDatabase(db.engine)


    @property
    def schema_table_name(self):
        return f"data_frame_content_{self.id}"


    @property
    def content_table_name(self):
        return f'{DATA_FRAME_CONTENT_SCHEMA}."{self.schema_table_name}"'


    def _add_record_ids(self, content: pd.DataFrame):
        content = content.reset_index(drop=True)
        data_frame_id = uuid.UUID(str(self.id))
        record_ids = [str(uuid.uuid5(data_frame_id, str(idx))) for idx in content.index]
        content.index = pd.Index(record_ids, name=RECORD_IDS_HEADER)
        return content


    def _refresh_db_content(self):
        if self._content_table.exists() and self._content_columns_match_db_columns():
            # drop content table instead of deleting content rows to
            # avoid auto-incrementing index from having weird values
            self._drop_content_table()
            self._content_table.create()
        else:
            self._content_table.create()
        self._content_table.insert(chunksize=CHUNK_SIZE)
        self._should_update_db_content = False


    def save(self):
        try:
            db.session.add(self)
            db.session.commit()
        except DataError as e:
            db.session.rollback()
            raise DataFrameMetadataSaveError(
                f"Failed to write metadata to 'file_content' table;"
                f" upload_file_id: {self.upload_file_id}."
            ) from e

        if self._should_update_db_content:
            try:
                self._refresh_db_content()
            except ProgrammingError as e:
                raise DataFrameMutableContentSaveError(
                    f"Failed to persist data frame content to database for data frame {self.id}."
                ) from e


    def delete(self):
        super().delete()
        self._sql_engine.drop_table(
            table_name=self.schema_table_name, schema=DATA_FRAME_CONTENT_SCHEMA
        )


    @property
    def content(self):
        if self._content is None:
            # refresh _content cache from db content
            chunks = pd.read_sql(
                sql=f"""
                    select * from {self.content_table_name}
                    {
                        f"where not {IGNORE_ROW_HEADER}"
                        if IGNORE_ROW_HEADER in self._db_content_columns.columns
                        else ""
                    }
                """,
                con=db.engine,
                chunksize=CHUNK_SIZE,
            )
            db_content = next(chunks)
            for chunk in chunks:
                db_content = db_content.append(chunk)
            self._content = self._content_from_db_content(db_content)
            
        return self._content


    def _content_from_db_content(
        self,
        db_content,
        index_by_record_ids=True,
        include_metadata_column=False,
    ):
        content = db_content

        if not include_metadata_column:
            columns_to_drop.append(METADATA_HEADER)

        if index_by_record_ids:
            content = content.drop([INDEX_HEADER], axis=1, errors="ignore")
            if RECORD_IDS_HEADER in content.columns:
                # Move record IDs from column to index.
                content.set_index(RECORD_IDS_HEADER, inplace=True)
        elif INDEX_HEADER in content.columns:
            content.set_index(INDEX_HEADER, inplace=True)
            content.index = content.index - 1

        return content


    def update_content(self, other: pd.DataFrame) -> None:
        if not other.index.name == RECORD_IDS_HEADER:
            if RECORD_IDS_HEADER not in other.columns:
                raise ValueError("Expected other data frame to have record ids, none found.")
            other = other.set_index(RECORD_IDS_HEADER)

        self.content.update(other)

        self._should_update_db_content = True


    def exec_sql_update(self, sql: TextClause) -> None:
        if self._should_update_db_content:
            # handle the case where file_content.content is updated, but a call to
            # file_content.save() hasn't occurred yet
            self._refresh_db_content()
        with db.engine.connect() as connection:
            connection.execute(sql)
        self._content = None


    def exec_sql_read(self, sql: TextClause, index_by_record_ids=True) -> pd.DataFrame:
        if self._should_update_db_content:
            # handle the case where file_content.content is updated, but a call to
            # file_content.save() hasn't occurred yet
            self._refresh_db_content()
        db_content = pd.read_sql(sql=sql, con=db.engine)
        return self._content_from_db_content(db_content, index_by_record_ids=index_by_record_ids)