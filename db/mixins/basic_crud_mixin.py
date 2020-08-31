from typing import List, Union
from uuid import UUID

from sqlalchemy import cast, func, String
from sqlalchemy.exc import DataError, IntegrityError, ProgrammingError
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from extensions import db


class BasicCrudMixin:
    def save(self):
        try:
            db.session.add(self)
            db.session.commit()
        except (DataError, IntegrityError):
            # on rollback, the same closure of state
            # as that of commit proceeds.
            db.session.rollback()
            raise

    def delete(self):
        try:
            db.session.delete(self)
            db.session.commit()
        except DataError:
            # on rollback, the same closure of state
            # as that of commit proceeds.
            db.session.rollback()
            raise

    @classmethod
    def get_one(cls, eager=False, **filter):
        if id is None:
            raise TypeError
        try:
            if eager:
                rec = cls.query.options(selectinload("*")).filter_by(**filter).first()
            else:  # lazy
                rec = cls.query.filter_by(**filter).first()
            return rec
        except DataError:
            db.session.rollback()
            raise
        except ProgrammingError:
            raise

    @classmethod
    def get_many(cls, eager=False, **filter):
        if id is None:
            raise TypeError
        try:
            if eager:
                rec = cls.query.options(selectinload("*")).filter_by(**filter).all()
            else:  # lazy
                rec = cls.query.filter_by(**filter).all()
            return rec
        except DataError:
            db.session.rollback()
            raise
        except ProgrammingError:
            raise

    @classmethod
    def get_many_by_primary_keys(cls, primary_keys: List[Union[UUID, str]]):
        try:
            primary_keys_as_strings = [str(pk) for pk in primary_keys]
            primary_keys_as_column = func.unnest(primary_keys_as_strings).label("pk")
            primary_keys_as_cte = select([primary_keys_as_column]).cte("primary_keys")

            rec = (
                cls.query
                .join(primary_keys_as_cte, cast(cls.id, String) == primary_keys_as_cte.c.pk)
                .all()
            )
            return rec
        except DataError:
            db.session.rollback()
            raise
        except ProgrammingError:
            raise
