#!/usr/bin/python
# -*- coding: utf-8 -*-

import flickrapi
import webbrowser
import os
import datetime
import logging
from database import FlickrPhoto, FlickrPhotoSet, FlickrPhotoSetPhoto, DatabaseUtil
from retry import retry

PHOTO_EXTRAS = "date_upload,date_taken,original_format,last_update,tags,machine_tags,o_dims,views,media,url_o"


class FlickrOrganizerError(Exception):
    pass


class Flickr:
    def __init__(self, db):
        api_key = os.environ['FLICKR_API_KEY']
        api_secret = os.environ['FLICKR_API_SECRET']
        self.flickr = flickrapi.FlickrAPI(api_key, api_secret, format='parsed-json')
        self.user = self.authenticate()
        self.user_id = self.user['user']['id']
        self.db = db
        logging.basicConfig()

    def authenticate(self):
        print('Authenticate')

        # Only do this if we don't have a valid token already
        if not self.flickr.token_valid(perms='read'):

            # Get a request token
            self.flickr.get_request_token(oauth_callback='oob')

            # Open a browser at the authentication URL. Do this however
            # you want, as long as the user visits that URL.
            authorize_url = self.flickr.auth_url(perms='delete')
            webbrowser.open_new_tab(authorize_url)

            # Get the verifier code from the user. Do this however you
            # want, as long as the user gives the application the code.
            verifier = str(input('Verifier code: '))

            # Trade the request token for an access token
            self.flickr.get_access_token(verifier)

        print('Get user')
        user = self.flickr.test.login()
        if user['stat'] != "ok":
            raise FlickrOrganizerError("Login status not ok!")
        return user

    @staticmethod
    def flickr_timestamp2dt(value):
        return datetime.datetime.fromtimestamp(int(value))

    @retry(flickrapi.FlickrError, delay=1, backoff=2, tries=3)
    def walk_api_items(self, api, value_attribute, items_attribute, api_kwargs):
        page = 1
        total_pages = 1
        while page <= total_pages:
            api_kwargs['page'] = page
            api_result = api(**api_kwargs)
            if api_result['stat'] != "ok":
                raise FlickrOrganizerError("Result status wasn't ok!")

            api_result = api_result[value_attribute]
            total_pages = api_result['pages']
            for item in api_result[items_attribute]:
                yield item

            page += 1

    def _save_photo(self, flickr_photo):
        # Fake the taken timestamp of videos.
        title = flickr_photo["title"]
        taken_timestamp = datetime.datetime.strptime(flickr_photo["datetaken"], "%Y-%m-%d %H:%M:%S")
        taken_timestamp = self.photo_corrected_taken_timestamp(taken_timestamp, title)
        FlickrPhoto.create(
            photo_id=flickr_photo["id"],
            title=title,
            original_format=flickr_photo["originalformat"],
            tags=flickr_photo["tags"],
            machine_tags=flickr_photo["machine_tags"],
            media=flickr_photo["media"],
            media_status=flickr_photo["media_status"],
            view_count=flickr_photo["views"],
            taken_timestamp=taken_timestamp,
            uploaded_timestamp=self.flickr_timestamp2dt(flickr_photo["dateupload"]),
            updated_timestamp=self.flickr_timestamp2dt(flickr_photo["lastupdate"]),
            url_original=flickr_photo["url_o"],
            width=flickr_photo["width_o"],
            height=flickr_photo["height_o"]).save()

    def update_photos(self, ignore_photo_in_many_sets, ignore_photo_in_no_sets):
        FlickrPhoto.truncate_table()
        FlickrPhotoSetPhoto.truncate_table()
        photo_ids = set()
        sets = FlickrPhotoSet.select()
        if sets.count == 0:
            raise FlickrOrganizerError("No photo sets in the database.")

        with self.db.db.atomic():
            # Loop photos not in sets
            for photo in self.walk_api_items(
                self.flickr.photos.getNotInSet,
                "photos",
                "photo",
                    {
                        "user_id": self.user_id,
                        "extras": PHOTO_EXTRAS
                    }):

                if not ignore_photo_in_no_sets:
                    print(f"Photo {photo['id']} not in any set!")

                photo_ids.add(photo['id'])
                self._save_photo(photo)

            for db_set in sets:
                order_num = 1
                try:
                    for photo in self.walk_api_items(
                            self.flickr.photosets.getPhotos,
                            "photoset",
                            "photo", {
                                "user_id": self.user_id,
                                "photoset_id": db_set.photoset_id,
                                "extras": PHOTO_EXTRAS
                            }):
                        photo_id = photo["id"]

                        FlickrPhotoSetPhoto.create(
                            photoset_id = db_set.photoset_id,
                            photo_id = photo_id,
                            order_num = order_num
                        ).save()
                        order_num += 1

                        if photo_id in photo_ids:
                            if not ignore_photo_in_many_sets:
                                print(f"Photo {photo_id} in many sets")
                            continue

                        photo_ids.add(photo_id)
                        self._save_photo(photo)
                except flickrapi.FlickrError as ex:
                    raise FlickrOrganizerError(f"Failed to retrieve photos of set {db_set.photoset_id}. "
                                               f"Inner exception ({type(ex).__name__}): {ex}")

    def update_photo_sets(self):
        FlickrPhotoSet.truncate_table()
        with self.db.db.atomic():
            for set in self.walk_api_items(
                self.flickr.photosets.getList,
                    "photosets",
                    "photoset", {}):
                db_set = FlickrPhotoSet.create(
                    photoset_id=set['id'],
                    title=set['title']['_content'],
                    view_count=set['count_views'],
                    comment_count=set['count_comments'],
                    photo_count=set['count_photos'],
                    video_count=set['count_videos'],
                    updated_timestamp=self.flickr_timestamp2dt(set['date_update']),
                    created_timestamp=self.flickr_timestamp2dt(set['date_create']))
                db_set.save()

    def delete_duplicates(self, dry_run):
        duplicates = DatabaseUtil.get_duplicate_photos()
        for duplicate in duplicates:
            if dry_run:
                print(f"Would delete photo {duplicate.photo_id} with title {duplicate.title} taken at {duplicate.taken_timestamp}!")
                continue

            print(f"Deleting photo {duplicate.photo_id} with title {duplicate.title} taken at {duplicate.taken_timestamp}!")
            self.flickr.photos.delete(photo_id = duplicate.photo_id)
        # Must update photos table after removing duplicates!
        if not dry_run:
            self.update_photos(True, True)

    def fix_ordering_of_sets(self, dry_run):
        set_info = DatabaseUtil.get_sets_and_earliest_photo_date()
        for photo_set in set_info:
            title_prefix = photo_set.title[:10]
            min_date = photo_set.earliest_taken_timestamp.strftime("%Y-%m-%d")
            if title_prefix == min_date:
                continue

            print(f"Name of the set '{photo_set.title}' ({photo_set.photoset_id}) should begin with date {min_date}!")
            if dry_run:
                continue

            prefix_is_valid_date = True
            try:
                datetime.datetime.strptime(title_prefix, "%Y-%m-%d")
            except ValueError:
                prefix_is_valid_date = False

            if prefix_is_valid_date:
                new_title = min_date + photo_set.title[10:]
            else:
                new_title = f"{min_date} {photo_set.title}"

            self.flickr.photosets.editMeta(photoset_id=photo_set.id, title=new_title)

    @staticmethod
    def photo_corrected_taken_timestamp(taken_timestamp, title):
        try:
            if title.startswith('VID_'):
                return datetime.datetime.strptime(title[:19], 'VID_%Y%m%d_%H%M%S')
            if title.startswith('VID-') and title[12:15] == '-WA':
                return datetime.datetime.strptime(title[:12], 'VID-%Y%m%d')
        except ValueError as ve:
            pass
        return taken_timestamp

    def fix_ordering_of_photos(self):
        photo_sets = DatabaseUtil.get_sets()
        for photo_set in photo_sets:
            photos = DatabaseUtil.get_set_photos(photo_set.photoset_id)
            # Convert result to list
            photos = [photo for photo in photos]
            photos.sort(key=lambda x: x.taken_timestamp)
            photo_ids = ",".join([str(photo.photo_id) for photo in photos])

            print(f"Fix ordering of photos of photo set {photo_set.photoset_id}.")
            self.flickr.photosets.reorderPhotos(
                photoset_id = photo_set.photoset_id,
                photo_ids = photo_ids
            )
