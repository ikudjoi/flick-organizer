#!/usr/bin/python
# -*- coding: utf-8 -*-

from flickr import Flickr
from database import Database
from downloader import Downloader
import argparse
import logging
import os


class FlickrOrganizerError(Exception):
    pass


def parse_arguments():
    """
    Parse command line arguments
    :return: argparse.ArgumentParser object
    """
    parser = argparse.ArgumentParser(description="Flickr Organizer!")
    parser.add_argument('--photos', action='store_true', help="Update photos table")
    parser.add_argument('--sets', action='store_true', help="Update photo sets table")
    parser.add_argument('--ignore-photo-in-many-sets', action = 'store_true', help = "Do not issue a warning if a photo belongs to more than one set.")
    parser.add_argument('--ignore-photo-in-no-sets', action = 'store_true', help = "Do not issue a warning if a photo doesn't belongs to any set.")
    parser.add_argument('--delete-duplicates', action = 'store_true', help = "Delete duplicate photos.")
    parser.add_argument('--dry-run', action = 'store_true', help = "Combine with --delete-duplicates, will only show what would be done.")
    parser.add_argument('--order-sets', action = 'store_true', help = "Reorder sets by taken date")
    parser.add_argument('--order-photos', action = 'store_true', help = "Reorder photos by taken date.")
    parser.add_argument('--download-path', type = str, help = "Download all photos from the Flickr Account to given local directory")
    parser.add_argument('--move-date-taken', type = str, help = "Move date taken of given photos (comma-separated string of ids)")
    parser.add_argument('--time-delta', type = str, help = "Time delta for move-date-taken parameter as datetime.timedelta parameters")
    return parser.parse_args()


logging.basicConfig(filename='connector.log',filemode='w',level=logging.DEBUG,format='%(asctime)s %(message)s')
logging.debug('Get user photos')

db = Database()
flickr = Flickr(db)

failedphoto = None

def escape_apostrophe(str):
    return str.replace('\'','\'\'')

def remove_extension(filename):
    lower = filename.lower()
    if lower.endswith('.jpeg'):
        return filename[:-5]
    if lower.endswith('.jpg') or lower.endswith('.mov') or lower.endswith('.avi'):
        return filename[:-4]
    return filename


def order_photosets_by_taken_date():
    logging.debug('Get photoset ids ordered by photo taken date desc.')
    sql = """select ps.id
    from f_photoset ps
    inner join f_photosetphoto psp on ps.id = psp.photosetId
    left join f_photosetnodate nd on ps.id = nd.id
    inner join f_photo p on psp.photoId = p.id
    group by ps.id, ps.title order by max(case when nd.id is not null then cast('1900-01-01' as datetime) else p.taken end) desc"""

    setids = db.get_int_list_from_database(sql)
    setids = [str(i) for i in setids]
    logging.debug('Retrieved %s photoset ids.' % len(setids))
    flickr.Photoset().orderSets(photoset_ids=','.join(setids))

    logging.debug('Ordering photos inside sets. Getting list of sets.')
    photosets = flickr.get_photosets()
    for photoset in photosets:
        logging.debug('Getting photos of set %s and ordering them by taken date.' % photoset.title)
        sql = """select p.id from f_photo p
                 inner join f_photosetphoto psp on p.id = psp.photoId
                 where psp.photosetId = %s
                 order by p.taken asc""" % photoset.id
        photoids = db.get_int_list_from_database(sql)
        photoids = [str(i) for i in photoids]
        try:
            photoset.reorderPhotos(photo_ids=','.join(photoids))
        except flickr.FlickrError as e:
            logging.warn("Could not reorder photoset %s.", photoset.id)


arguments = parse_arguments()
dry_run = arguments.dry_run

if arguments.sets:
    flickr.update_photosets()
if arguments.photos:
    ignore_photo_in_many_sets = arguments.ignore_photo_in_many_sets
    ignore_photo_in_no_sets = arguments.ignore_photo_in_no_sets
    flickr.update_photos(ignore_photo_in_many_sets, ignore_photo_in_no_sets)
if arguments.delete_duplicates:
    flickr.delete_duplicates(dry_run)
if arguments.order_sets:
    flickr.fix_ordering_of_sets(dry_run)
if arguments.order_photos:
    flickr.fix_ordering_of_photos()
if arguments.move_date_taken is not None:
    download_path = arguments.download_path
    if download_path is None:
        raise FlickrOrganizerError("Must provide --download-path argument too!")
    time_delta = arguments.time_delta
    if time_delta is None:
        raise FlickrOrganizerError("Must provide --time-delta argument too!")
    downloader = Downloader(download_path, dry_run)
    downloader.move_date_taken(arguments.move_date_taken, time_delta)
# Do not trigger download of all photos if download path was given for moving taken timestamps.
elif arguments.download_path is not None:
    download_path = arguments.download_path
    downloader = Downloader(download_path, dry_run)
    downloader.download()
