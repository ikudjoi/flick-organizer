#!/usr/bin/python
# -*- coding: utf-8 -*-

import flickrapi
import webbrowser
import os
import datetime
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
        self.authenticate()
        self.db = db

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
        self.user = self.flickr.test.login()
        if self.user['stat'] != "ok":
            raise FlickrOrganizerError("Login status not ok!")
        self.user_id = self.user['user']['id']

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
        FlickrPhoto.create(
            photo_id=flickr_photo["id"],
            title=flickr_photo["title"],
            original_format=flickr_photo["originalformat"],
            tags=flickr_photo["tags"],
            machine_tags=flickr_photo["machine_tags"],
            media=flickr_photo["media"],
            media_status=flickr_photo["media_status"],
            view_count=flickr_photo["views"],
            taken_timestamp=datetime.datetime.strptime(flickr_photo["datetaken"], "%Y-%m-%d %H:%M:%S"),
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

            for dbset in sets:
                order_num = 1
                try:
                    for photo in self.walk_api_items(
                            self.flickr.photosets.getPhotos,
                            "photoset",
                            "photo", {
                                "user_id": self.user_id,
                                "photoset_id": dbset.photoset_id,
                                "extras": PHOTO_EXTRAS
                            }):
                        photo_id = photo["id"]

                        FlickrPhotoSetPhoto.create(
                            photoset_id = dbset.photoset_id,
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
                    raise FlickrOrganizerError(f"Failed to retrieve photos of set {dbset.photoset_id}. Inner exception: {ex}")

    def update_photosets(self):
        FlickrPhotoSet.truncate_table()
        with self.db.db.atomic():
            for set in self.walk_api_items(
                self.flickr.photosets.getList,
                    "photosets",
                    "photoset", {}):
                dbset = FlickrPhotoSet.create(
                    photoset_id=set['id'],
                    title=set['title']['_content'],
                    view_count=set['count_views'],
                    comment_count=set['count_comments'],
                    photo_count=set['count_photos'],
                    video_count=set['count_videos'],
                    updated_timestamp=self.flickr_timestamp2dt(set['date_update']),
                    created_timestamp=self.flickr_timestamp2dt(set['date_create']))
                dbset.save()

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
        for set in set_info:
            title_prefix = set.title[:10]
            min_date = set.earliest_taken_timestamp.strftime("%Y-%m-%d")
            if title_prefix == min_date:
                continue

            if dry_run:
                print(f"Name of the set '{set.title}' ({set.photoset_id}) should begin with date {min_date}!")
                continue

            prefix_is_valid_date = True
            try:
                datetime.datetime.strptime(title_prefix, "%Y-%m-%d")
            except ValueError:
                prefix_is_valid_date = False

            if prefix_is_valid_date:
                new_title = min_date + set.title[10:]
            else:
                new_title = f"{min_date} {set.title}"

            self.flickr.photosets.editMeta(title = new_title)
