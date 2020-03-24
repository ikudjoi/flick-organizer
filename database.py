#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
from peewee import *

db = SqliteDatabase(os.environ['SQLITE_DATABASE'])


class BaseModel(Model):
    class Meta:
        database = db


class FlickrPhoto(BaseModel):
    photo_id = IntegerField(index=True, unique=True)
    title = TextField()
    original_format = TextField()
    tags = TextField()
    machine_tags = TextField()
    media = TextField()
    media_status = TextField()
    view_count = IntegerField()
    taken_timestamp = DateTimeField()
    uploaded_timestamp = DateTimeField()
    updated_timestamp = DateTimeField()
    url_original = TextField()
    width = IntegerField()
    height = IntegerField()


class FlickrPhotoSet(BaseModel):
    photoset_id = IntegerField(index=True, unique=True)
    title = TextField()
    created_timestamp = DateTimeField()
    comment_count = IntegerField()
    view_count = IntegerField()
    photo_count = IntegerField()
    video_count = IntegerField()
    updated_timestamp = DateTimeField()
    created_timestamp = DateTimeField()


#photoset nodate ?
class FlickrPhotoSetPhoto(BaseModel):
    photo_id = ForeignKeyField(FlickrPhoto)
    photoset_id = ForeignKeyField(FlickrPhotoSet)
    order_num = IntegerField()


class LocalPhoto(BaseModel):
    file_path = TextField(index=True, unique=True)
    title = TextField()
    width = IntegerField()
    height = IntegerField()
    plain_timestamp = DateTimeField()
    digitized_timestamp = DateTimeField()
    original_timestamp = DateTimeField()
    unique_id = TextField()
    make = TextField()
    model = TextField(null=True)
    orientation = IntegerField(null=True)


class Database:
    def __init__(self):
        self.db = db
        self.db.connect()
        self.db.create_tables([FlickrPhoto, FlickrPhotoSet, FlickrPhotoSetPhoto, LocalPhoto])


class DatabaseUtil:
    @staticmethod
    def get_duplicate_photos():
        set_counts = (FlickrPhotoSetPhoto.select(
            FlickrPhotoSetPhoto.photo_id,
            fn.count().alias("set_count")
        ).group_by(FlickrPhotoSetPhoto.photo_id)).alias("set_counts")

        subquery = (FlickrPhoto.select(
            FlickrPhoto.photo_id,
            FlickrPhoto.width,
            FlickrPhoto.height,
            FlickrPhoto.taken_timestamp,
            FlickrPhoto.title,
            FlickrPhoto.url_original,
            FlickrPhoto.media,
            FlickrPhoto.media_status,
            FlickrPhoto.machine_tags,
            FlickrPhoto.tags,
            FlickrPhoto.original_format,
            set_counts.c.set_count,
            fn.row_number().over(
            partition_by=[FlickrPhoto.title, FlickrPhoto.taken_timestamp],
            # When choosing a duplicate, prefer ones with less set memberships and smaller sizes.
            order_by=[fn.ifnull(set_counts.c.set_count, 0).desc(), FlickrPhoto.width.desc()]).alias('rn'))
                    .join(set_counts, JOIN.LEFT_OUTER, on=(FlickrPhoto.photo_id == set_counts.c.photo_id))
                    .alias("enumerated"))

        # Since we can't filter on the rank, we are wrapping it in a query
        # and performing the filtering in the outer query.
        return (FlickrPhoto.select(
            subquery.c.photo_id,
            subquery.c.width,
            subquery.c.height,
            subquery.c.taken_timestamp,
            subquery.c.title,
            subquery.c.url_original,
            subquery.c.media,
            subquery.c.media_status,
            subquery.c.machine_tags,
            subquery.c.tags,
            subquery.c.original_format,
            subquery.c.set_count)
                 .from_(subquery)
                 .where(subquery.c.rn > 1))

    @staticmethod
    def get_sets_and_earliest_photo_date():
        return (FlickrPhotoSet.select(
            FlickrPhotoSet.photoset_id,
            FlickrPhotoSet.title,
            fn.min(FlickrPhoto.taken_timestamp).alias("earliest_taken_timestamp"))
                .join(FlickrPhotoSetPhoto, on=(FlickrPhotoSet.photoset_id == FlickrPhotoSetPhoto.photoset_id))
                .join(FlickrPhoto, on=(FlickrPhotoSetPhoto.photo_id == FlickrPhoto.photo_id))
                .group_by(FlickrPhotoSet.photoset_id, FlickrPhotoSet.title))

    @staticmethod
    def get_sets():
        return FlickrPhotoSet.select()

    @staticmethod
    def get_set_photos(set_id):
        return (FlickrPhoto.select(
            FlickrPhoto.photo_id,
            FlickrPhoto.title,
            FlickrPhoto.taken_timestamp,
            FlickrPhoto.url_original,
            FlickrPhoto.width,
            FlickrPhoto.height)
            .join(FlickrPhotoSetPhoto, on=(FlickrPhoto.photo_id == FlickrPhotoSetPhoto.photo_id))
            .where(FlickrPhotoSetPhoto.photoset_id == set_id))
