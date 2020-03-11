#!/usr/bin/python
# -*- coding: utf-8 -*-

import flickrapi
import webbrowser
import os
import datetime
from database import FlickrPhoto, FlickrPhotoSet, FlickrPhotoSetPhoto, LocalPhoto
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
            raise Exception("Login status not ok!")
        self.user_id = self.user['user']['id']

    def flickr_timestamp2dt(self, value):
        return datetime.datetime.fromtimestamp(int(value))

    @retry((flickrapi.FlickrError), delay=1, backoff=2, tries=3)
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
                        photo_id = photo_id
                    ).save()

                    if photo_id in photo_ids:
                        if not ignore_photo_in_many_sets:
                            print(f"Photo {photo_id} in many sets")
                        continue

                    photo_ids.add(photo_id)
                    self._save_photo(photo)

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
        duplos = self.db.get_duplicate_photos()
        for duplicate in duplos:
            if dry_run:
                print(f"Would delete photo {duplicate.photo_id} with title {duplicate.title} taken at {duplicate.taken_timestamp}!")
                continue

            print(f"Deleting photo {duplicate.photo_id} with title {duplicate.title} taken at {duplicate.taken_timestamp}!")
            self.flickr.photos.delete(photo_id = duplicate.photo_id)
        # Must update photos table after removing duplicates!
        if not dry_run:
            self.update_photos(True, True)


        # with con:
        #     # With this sql we get the date of the earliest photo per set.
        #     sql = """
        #     select ps.id, mintaken.prefix
        #     from f_photoset ps
        #     left join f_photosetnodate nd
        #     on ps.id = nd.id
        #     inner join (
        #     select psp.photosetid, date_format(min(fp.taken), '%Y-%m-%d') as prefix
        #     from f_photosetphoto psp
        #     inner join f_photo fp
        #     on psp.photoId = fp.id
        #     group by psp.photosetId) mintaken
        #     on ps.id = mintaken.photosetid
        #     where nd.id is null
        #     """
        #     cur.execute(sql)
        #
        #     setdates = dict()
        #     row = cur.fetchone()
        #     while row is not None:
        #         setdates[str(row[0])] = row[1]
        #         row = cur.fetchone()
        #
        #     photosets = user.getPhotosets()
        #     for ps in photosets:
        #         if ps.id in setdates:
        #             prefix = setdates[ps.id]
        #             if not ps.title.startswith(prefix):
        #                 title = ps.title
        #                 if (re.match('\d\d\d\d-\d\d-\d\d .*', ps.title)):
        #                     title = title[11:]
        #                 title = prefix + ' ' + title
        #                 logging.debug('Changing photoset %s title ''%s'' to ''%s''.' % (ps.id, ps.title, title))
        #                 #ps.editMeta(title = title)
        #
        #     cur.execute('TRUNCATE TABLE f_photoset;')
        #     logging.debug('Build command')
        #     cmd = u'INSERT INTO f_photoset (id, title, createDate, commentCount, viewCount, updateDate) ' + \
        #           u'VALUES (' + u'),('.join([photoset_entry(photoset) for photoset in photosets]) + u');'
        #     logging.debug('Execute command')
        #     cur.execute(cmd)
        #     con.commit()
        #     logging.debug('Inserted %s photosets to the database.' % len(photosets))
        #
        #     afile = codecs.open('albums.txt', 'w', 'utf-8')
        #     cur.execute('TRUNCATE TABLE f_photosetphoto;')
        #     photosetphotos = []
        #     for photoset in sorted(photosets, key=lambda ps: ps.title, reverse=True):
        #
        #         line = u'<li><a href="https://www.flickr.com/photos/ilkkakudjoi/sets/%s/">%s</a></li>\n' % (photoset.id, photoset.title)
        #         afile.write(line)
        #
        #         page = 1
        #         while True:
        #             photos = photoset.getPhotos(page=page)
        #             logging.debug('Retrieved %s photo ids from set %s.' % (len(photos), photoset.title))
        #             photosetphotos += [(photo.id, photoset.id) for photo in photos]
        #             page += 1
        #             if (page > photos.info.pages):
        #                 break
        #     afile.close()
        #     cmd = u'INSERT INTO f_photosetphoto (photoId, photosetId) ' + \
        #           u'VALUES (' + u'),('.join([psp[0] + ', ' + psp[1] for psp in photosetphotos]) + u');'
        #     cur.execute(cmd)
        #     try:
        #         con.commit()
        #     except (Exception):
        #         con.commit()
        #     logging.debug('Inserted %s photo-photoset links to the database.' % len(photosetphotos))
